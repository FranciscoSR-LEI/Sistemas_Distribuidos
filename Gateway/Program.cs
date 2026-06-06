using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using Microsoft.Data.SqlClient;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace GatewayApp;

public class MonitoringMessage
{
    public string MessageType { get; set; } = "";
    public string Timestamp { get; set; } = "";
    public string SensorId { get; set; } = "";
    public string Zona { get; set; } = "";
    public string Tipo { get; set; } = "";
    public string Valor { get; set; } = "";
    public string DataFormat { get; set; } = "";
    public string RawPayload { get; set; } = "";
    public string FileName { get; set; } = "";
    public string FileContentBase64 { get; set; } = "";
}

public class SensorRegistration
{
    public int Id { get; set; }
    public string SensorId { get; set; } = "";
    public string Estado { get; set; } = "";
    public string Zona { get; set; } = "";
    public string Tipos { get; set; } = "";
    public DateTime RegisteredAt { get; set; }
    public DateTime LastSync { get; set; }

    public List<string> GetTiposList()
    {
        List<string> result = new();
        if (string.IsNullOrWhiteSpace(Tipos))
            return result;

        foreach (string tipo in Tipos.Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            result.Add(tipo.Trim());
        }

        return result;
    }
}

public class PreprocessRequest
{
    public string Tipo { get; set; } = "";
    public string DataFormat { get; set; } = "";
    public string RawPayload { get; set; } = "";
}

public class PreprocessResponse
{
    public bool Ok { get; set; }
    public string NormalizedValue { get; set; } = "";
    public string Notes { get; set; } = "";
}

public class GatewayOptions
{
    public string InstanceName { get; }
    public string Zona { get; }
    public string QueueName { get; }
    public string ExchangeName { get; }
    public string LogFile { get; }

    public GatewayOptions(string instanceName, string zona)
    {
        InstanceName = instanceName;
        Zona = zona;
        ExchangeName = "urban.topic";
        QueueName = $"queue.{zona.ToLowerInvariant()}";
        LogFile = $"gateway_{instanceName.ToLowerInvariant()}_{zona.ToLowerInvariant()}.log";
    }
}

public interface ILoggerService
{
    void Write(string message);
}

public class FileLoggerService : ILoggerService
{
    private readonly string _logFile;
    private readonly object _lockObj = new();

    public FileLoggerService(string logFile)
    {
        _logFile = logFile;
    }

    public void Write(string message)
    {
        lock (_lockObj)
        {
            string line = $"{DateTime.Now:s} {message}";
            File.AppendAllText(_logFile, line + Environment.NewLine);
        }
    }
}

public class SensorRepository
{
    private readonly string _connectionString;
    private readonly object _lockObj = new();

    public SensorRepository(string connectionString)
    {
        _connectionString = connectionString;
    }

    public void EnsureTable()
    {
        lock (_lockObj)
        {
            using SqlConnection connection = new(_connectionString);
            connection.Open();

            string sql = @"
IF OBJECT_ID('dbo.Sensors', 'U') IS NULL
BEGIN
    CREATE TABLE Sensors (
        Id INT IDENTITY(1,1) PRIMARY KEY,
        SensorId NVARCHAR(50) NOT NULL UNIQUE,
        Estado NVARCHAR(50) NOT NULL,
        Zona NVARCHAR(100) NOT NULL,
        Tipos NVARCHAR(200) NOT NULL,
        RegisteredAt DATETIME2 NOT NULL,
        LastSync DATETIME2 NOT NULL
    );
END";

            using SqlCommand cmd = new(sql, connection);
            cmd.ExecuteNonQuery();
        }
    }

    public SensorRegistration? GetBySensorId(string sensorId)
    {
        lock (_lockObj)
        {
            using SqlConnection connection = new(_connectionString);
            connection.Open();

            string sql = @"
SELECT Id, SensorId, Estado, Zona, Tipos, RegisteredAt, LastSync
FROM Sensors
WHERE SensorId = @SensorId;";

            using SqlCommand cmd = new(sql, connection);
            cmd.Parameters.AddWithValue("@SensorId", sensorId);

            using SqlDataReader reader = cmd.ExecuteReader();
            if (!reader.Read())
                return null;

            return new SensorRegistration
            {
                Id = reader.GetInt32(0),
                SensorId = reader.GetString(1),
                Estado = reader.GetString(2),
                Zona = reader.GetString(3),
                Tipos = reader.GetString(4),
                RegisteredAt = reader.GetDateTime(5),
                LastSync = reader.GetDateTime(6)
            };
        }
    }

    public void RegisterIfNotExists(string sensorId, string zona, string tipos)
    {
        lock (_lockObj)
        {
            using SqlConnection connection = new(_connectionString);
            connection.Open();

            string checkSql = "SELECT COUNT(*) FROM Sensors WHERE SensorId = @SensorId;";
            using SqlCommand checkCmd = new(checkSql, connection);
            checkCmd.Parameters.AddWithValue("@SensorId", sensorId);

            int count = Convert.ToInt32(checkCmd.ExecuteScalar());
            if (count > 0)
                return;

            string insertSql = @"
INSERT INTO Sensors (SensorId, Estado, Zona, Tipos, RegisteredAt, LastSync)
VALUES (@SensorId, @Estado, @Zona, @Tipos, @RegisteredAt, @LastSync);";

            using SqlCommand insertCmd = new(insertSql, connection);
            insertCmd.Parameters.AddWithValue("@SensorId", sensorId);
            insertCmd.Parameters.AddWithValue("@Estado", "ativo");
            insertCmd.Parameters.AddWithValue("@Zona", zona);
            insertCmd.Parameters.AddWithValue("@Tipos", tipos);
            insertCmd.Parameters.AddWithValue("@RegisteredAt", DateTime.Now);
            insertCmd.Parameters.AddWithValue("@LastSync", DateTime.Now);

            insertCmd.ExecuteNonQuery();
        }
    }

    public void UpdateLastSync(string sensorId)
    {
        lock (_lockObj)
        {
            using SqlConnection connection = new(_connectionString);
            connection.Open();

            string sql = @"
UPDATE Sensors
SET LastSync = @LastSync
WHERE SensorId = @SensorId;";

            using SqlCommand cmd = new(sql, connection);
            cmd.Parameters.AddWithValue("@LastSync", DateTime.Now);
            cmd.Parameters.AddWithValue("@SensorId", sensorId);

            cmd.ExecuteNonQuery();
        }
    }

    public List<string> GetBindingsForZone(string zona)
    {
        List<string> bindings = new();
        HashSet<string> uniqueBindings = new(StringComparer.OrdinalIgnoreCase);

        lock (_lockObj)
        {
            using SqlConnection connection = new(_connectionString);
            connection.Open();

            string sql = @"
SELECT SensorId, Estado, Zona, Tipos
FROM Sensors
WHERE UPPER(Zona) = UPPER(@Zona);";

            using SqlCommand cmd = new(sql, connection);
            cmd.Parameters.AddWithValue("@Zona", zona);

            using SqlDataReader reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                string estado = reader.GetString(1);
                string zonaDb = reader.GetString(2);
                string tipos = reader.GetString(3);

                if (!estado.Equals("ativo", StringComparison.OrdinalIgnoreCase))
                    continue;

                uniqueBindings.Add($"{zonaDb}.HELLO");
                uniqueBindings.Add($"{zonaDb}.HEARTBEAT");
                uniqueBindings.Add($"{zonaDb}.BYE");
                uniqueBindings.Add($"{zonaDb}.VIDEO");

                foreach (string tipo in tipos.Split(',', StringSplitOptions.RemoveEmptyEntries))
                {
                    uniqueBindings.Add($"{zonaDb}.{tipo.Trim()}");
                }
            }
        }

        bindings.AddRange(uniqueBindings);
        return bindings;
    }
}

public class HeartbeatRegistry
{
    private readonly Dictionary<string, DateTime> _lastHeartbeats = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _inactiveAlerted = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _lockObj = new();

    public void Update(string sensorId)
    {
        lock (_lockObj)
        {
            _lastHeartbeats[sensorId] = DateTime.Now;
            _inactiveAlerted.Remove(sensorId);
        }
    }

    public List<string> GetInactiveSensors()
    {
        List<string> result = new();

        lock (_lockObj)
        {
            foreach (var kvp in _lastHeartbeats)
            {
                if ((DateTime.Now - kvp.Value).TotalSeconds > 30 && !_inactiveAlerted.Contains(kvp.Key))
                {
                    result.Add(kvp.Key);
                    _inactiveAlerted.Add(kvp.Key);
                }
            }
        }

        return result;
    }
}

public class PreprocessRpcClient
{
    private readonly string _host;
    private readonly int _port;
    private readonly ILoggerService _logger;

    public PreprocessRpcClient(string host, int port, ILoggerService logger)
    {
        _host = host;
        _port = port;
        _logger = logger;
    }

    public PreprocessResponse Normalize(string tipo, string dataFormat, string rawPayload)
    {
        try
        {
            using TcpClient rpcClient = new(_host, _port);
            using NetworkStream ns = rpcClient.GetStream();
            using StreamWriter writer = new(ns) { AutoFlush = true };
            using StreamReader reader = new(ns);

            PreprocessRequest request = new()
            {
                Tipo = tipo,
                DataFormat = dataFormat,
                RawPayload = rawPayload
            };

            writer.WriteLine(JsonSerializer.Serialize(request));

            string? responseJson = reader.ReadLine();
            if (responseJson == null)
                return new PreprocessResponse { Ok = false };

            PreprocessResponse? response = JsonSerializer.Deserialize<PreprocessResponse>(responseJson);
            return response ?? new PreprocessResponse { Ok = false };
        }
        catch (Exception ex)
        {
            _logger.Write("Erro RPC pré-processamento: " + ex.Message);
            return new PreprocessResponse { Ok = false };
        }
    }
}

public class ServerForwarder
{
    private readonly string _host;
    private readonly int _port;
    private readonly ILoggerService _logger;

    public ServerForwarder(string host, int port, ILoggerService logger)
    {
        _host = host;
        _port = port;
        _logger = logger;
    }

    public bool SendStore(
        string timestamp,
        string sensorId,
        string zona,
        string tipo,
        string valor,
        string processedByGateway)
    {
        try
        {
            using TcpClient serverClient = new(_host, _port);
            serverClient.NoDelay = true;
            serverClient.ReceiveTimeout = 15000;
            serverClient.SendTimeout = 15000;

            using NetworkStream ns = serverClient.GetStream();
            using StreamWriter writer = new(ns) { AutoFlush = true };
            using StreamReader reader = new(ns);

            string message = $"STORE|{timestamp}|{sensorId}|{zona}|{tipo}|{valor}|{processedByGateway}";
            writer.WriteLine(message);

            string? response = reader.ReadLine();
            _logger.Write($"Encaminhado ao servidor: {message} | Resposta: {response}");

            return response != null && response.Contains("OK", StringComparison.OrdinalIgnoreCase);
        }
        catch (Exception ex)
        {
            _logger.Write("Erro ao ligar ao servidor: " + ex.Message);
            return false;
        }
    }

    public bool SendVideo(
        string timestamp,
        string sensorId,
        string zona,
        string fileName,
        byte[] videoBytes,
        string processedByGateway)
    {
        try
        {
            using TcpClient serverClient = new(_host, _port);
            serverClient.NoDelay = true;
            serverClient.ReceiveTimeout = 30000;
            serverClient.SendTimeout = 30000;

            using NetworkStream ns = serverClient.GetStream();
            using StreamReader reader = new(ns);
            using StreamWriter writer = new(ns) { AutoFlush = true };

            string header = $"VIDEO_UPLOAD|{timestamp}|{sensorId}|{zona}|{fileName}|{videoBytes.Length}|{processedByGateway}";
            writer.WriteLine(header);
            writer.Flush();

            string? ready = reader.ReadLine();
            if (ready == null || !ready.Equals("VIDEO_UPLOAD_ACK|READY", StringComparison.OrdinalIgnoreCase))
            {
                _logger.Write($"Servidor não ficou pronto para vídeo de {sensorId}. Resposta: {ready}");
                return false;
            }

            ns.Write(videoBytes, 0, videoBytes.Length);
            ns.Flush();

            string? finalAck = reader.ReadLine();
            _logger.Write($"Vídeo encaminhado ao servidor: {header} | Resposta: {finalAck}");

            return finalAck != null && finalAck.Equals("VIDEO_UPLOAD_ACK|OK", StringComparison.OrdinalIgnoreCase);
        }
        catch (Exception ex)
        {
            _logger.Write("Erro ao encaminhar vídeo ao servidor: " + ex.Message);
            return false;
        }
    }
}

public class GatewayMessageProcessor
{
    private readonly GatewayOptions _options;
    private readonly SensorRepository _sensorRepository;
    private readonly HeartbeatRegistry _heartbeatRegistry;
    private readonly PreprocessRpcClient _preprocessRpcClient;
    private readonly ServerForwarder _serverForwarder;
    private readonly ILoggerService _logger;

    public GatewayMessageProcessor(
        GatewayOptions options,
        SensorRepository sensorRepository,
        HeartbeatRegistry heartbeatRegistry,
        PreprocessRpcClient preprocessRpcClient,
        ServerForwarder serverForwarder,
        ILoggerService logger)
    {
        _options = options;
        _sensorRepository = sensorRepository;
        _heartbeatRegistry = heartbeatRegistry;
        _preprocessRpcClient = preprocessRpcClient;
        _serverForwarder = serverForwarder;
        _logger = logger;
    }

    public void Process(MonitoringMessage msg)
    {
        Console.WriteLine($"[{_options.InstanceName}] Recebido: type={msg.MessageType}, sensor={msg.SensorId}, zona={msg.Zona}, tipo={msg.Tipo}");

        switch (msg.MessageType)
        {
            case "HELLO":
                ProcessHello(msg);
                break;
            case "HEARTBEAT":
                ProcessHeartbeat(msg);
                break;
            case "DATA":
                ProcessData(msg);
                break;
            case "VIDEO":
                ProcessVideo(msg);
                break;
            case "BYE":
                ProcessBye(msg);
                break;
        }
    }

    private void ProcessHello(MonitoringMessage msg)
    {
        string tipos = msg.Tipo.Replace("[", "").Replace("]", "").Trim();

        _sensorRepository.RegisterIfNotExists(msg.SensorId, msg.Zona, tipos);

        SensorRegistration? sensor = _sensorRepository.GetBySensorId(msg.SensorId);
        if (sensor == null)
        {
            _logger.Write($"HELLO falhou: não foi possível registar/obter sensor {msg.SensorId}");
            return;
        }

        if (!sensor.Zona.Equals(msg.Zona, StringComparison.OrdinalIgnoreCase))
        {
            _logger.Write($"HELLO rejeitado: zona inválida para {msg.SensorId}");
            return;
        }

        if (!IsActive(sensor))
        {
            _logger.Write($"HELLO rejeitado: sensor {msg.SensorId} não está ativo");
            return;
        }

        _sensorRepository.UpdateLastSync(msg.SensorId);
        _heartbeatRegistry.Update(msg.SensorId);

        _logger.Write($"HELLO aceite de {msg.SensorId} na zona {msg.Zona} com tipos {sensor.Tipos}");
    }

    private void ProcessHeartbeat(MonitoringMessage msg)
    {
        SensorRegistration? sensor = _sensorRepository.GetBySensorId(msg.SensorId);
        if (sensor == null)
        {
            _logger.Write($"HEARTBEAT rejeitado: sensor {msg.SensorId} não existe");
            return;
        }

        if (!sensor.Zona.Equals(msg.Zona, StringComparison.OrdinalIgnoreCase))
        {
            _logger.Write($"HEARTBEAT rejeitado: zona inválida de {msg.SensorId}");
            return;
        }

        if (!IsActive(sensor))
        {
            _logger.Write($"HEARTBEAT rejeitado: sensor {msg.SensorId} inativo");
            return;
        }

        _sensorRepository.UpdateLastSync(msg.SensorId);
        _heartbeatRegistry.Update(msg.SensorId);

        _logger.Write($"HEARTBEAT de {msg.SensorId}");
    }

    private void ProcessData(MonitoringMessage msg)
    {
        SensorRegistration? sensor = _sensorRepository.GetBySensorId(msg.SensorId);
        if (sensor == null)
        {
            _logger.Write($"DATA rejeitado: sensor {msg.SensorId} não existe");
            return;
        }

        if (!sensor.Zona.Equals(msg.Zona, StringComparison.OrdinalIgnoreCase))
        {
            _logger.Write($"DATA rejeitado: zona inválida de {msg.SensorId}");
            return;
        }

        if (!IsActive(sensor))
        {
            _logger.Write($"DATA rejeitado: sensor {msg.SensorId} inativo");
            return;
        }

        if (!SupportsType(sensor, msg.Tipo))
        {
            _logger.Write($"DATA rejeitado: tipo {msg.Tipo} não suportado por {msg.SensorId}");
            return;
        }

        PreprocessResponse response = _preprocessRpcClient.Normalize(msg.Tipo, msg.DataFormat, msg.RawPayload);

        if (!response.Ok)
        {
            _logger.Write($"Pré-processamento falhou para {msg.SensorId}. Formato={msg.DataFormat}, Payload={msg.RawPayload}");
            return;
        }

        _sensorRepository.UpdateLastSync(msg.SensorId);
        _heartbeatRegistry.Update(msg.SensorId);

        bool ok = _serverForwarder.SendStore(
            msg.Timestamp,
            msg.SensorId,
            msg.Zona,
            msg.Tipo,
            response.NormalizedValue,
            _options.InstanceName);

        _logger.Write(ok
            ? $"DATA encaminhado para servidor: gateway={_options.InstanceName}, sensor={msg.SensorId}, tipo={msg.Tipo}, valor={response.NormalizedValue}"
            : $"Falha ao encaminhar DATA de {msg.SensorId}");
    }

    private void ProcessVideo(MonitoringMessage msg)
    {
        SensorRegistration? sensor = _sensorRepository.GetBySensorId(msg.SensorId);
        if (sensor == null)
        {
            _logger.Write($"VIDEO rejeitado: sensor {msg.SensorId} não existe");
            return;
        }

        if (!sensor.Zona.Equals(msg.Zona, StringComparison.OrdinalIgnoreCase))
        {
            _logger.Write($"VIDEO rejeitado: zona inválida de {msg.SensorId}");
            return;
        }

        if (!IsActive(sensor))
        {
            _logger.Write($"VIDEO rejeitado: sensor {msg.SensorId} inativo");
            return;
        }

        if (!SupportsType(sensor, "VIDEO"))
        {
            _logger.Write($"VIDEO rejeitado: sensor {msg.SensorId} não suporta VIDEO");
            return;
        }

        byte[] videoBytes = Convert.FromBase64String(msg.FileContentBase64);

        _sensorRepository.UpdateLastSync(msg.SensorId);
        _heartbeatRegistry.Update(msg.SensorId);

        bool ok = _serverForwarder.SendVideo(
            msg.Timestamp,
            msg.SensorId,
            msg.Zona,
            msg.FileName,
            videoBytes,
            _options.InstanceName);

        _logger.Write(ok
            ? $"VIDEO encaminhado para servidor: gateway={_options.InstanceName}, sensor={msg.SensorId}, ficheiro={msg.FileName}"
            : $"Falha ao encaminhar VIDEO de {msg.SensorId}");
    }

    private void ProcessBye(MonitoringMessage msg)
    {
        SensorRegistration? sensor = _sensorRepository.GetBySensorId(msg.SensorId);
        if (sensor == null)
        {
            _logger.Write($"BYE ignorado: sensor {msg.SensorId} não existe");
            return;
        }

        _sensorRepository.UpdateLastSync(msg.SensorId);
        _heartbeatRegistry.Update(msg.SensorId);

        _logger.Write($"BYE de {msg.SensorId}");
    }

    private static bool IsActive(SensorRegistration sensor)
    {
        if (sensor.Estado.Equals("desativado", StringComparison.OrdinalIgnoreCase))
            return false;

        if (sensor.Estado.Equals("manutencao", StringComparison.OrdinalIgnoreCase) ||
            sensor.Estado.Equals("manutenção", StringComparison.OrdinalIgnoreCase))
            return false;

        return sensor.Estado.Equals("ativo", StringComparison.OrdinalIgnoreCase);
    }

    private static bool SupportsType(SensorRegistration sensor, string tipo)
    {
        foreach (string t in sensor.GetTiposList())
        {
            if (t.Equals(tipo, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }
}

public class GatewayHost
{
    private readonly GatewayOptions _options;
    private readonly SensorRepository _sensorRepository;
    private readonly GatewayMessageProcessor _processor;
    private readonly HeartbeatRegistry _heartbeatRegistry;
    private readonly ILoggerService _logger;

    public GatewayHost(
        GatewayOptions options,
        SensorRepository sensorRepository,
        GatewayMessageProcessor processor,
        HeartbeatRegistry heartbeatRegistry,
        ILoggerService logger)
    {
        _options = options;
        _sensorRepository = sensorRepository;
        _processor = processor;
        _heartbeatRegistry = heartbeatRegistry;
        _logger = logger;
    }

    public void Run()
    {
        Console.WriteLine($"[{_options.InstanceName}] Gateway iniciado para a zona {_options.Zona}");
        Console.WriteLine($"[{_options.InstanceName}] Queue partilhada da zona: {_options.QueueName}");

        Thread monitorThread = new(MonitorHeartbeats)
        {
            IsBackground = true
        };
        monitorThread.Start();

        ConnectionFactory factory = new()
        {
            HostName = "localhost",
            UserName = "guest",
            Password = "guest"
        };

        using IConnection connection = factory.CreateConnection();
        using IModel channel = connection.CreateModel();

        channel.ExchangeDeclare(
            exchange: _options.ExchangeName,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false);

        channel.QueueDeclare(
            queue: _options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false);

        channel.BasicQos(0, 1, false);

        // bindings mínimos por zona
        string[] defaultBindings =
        {
            $"{_options.Zona}.HELLO",
            $"{_options.Zona}.HEARTBEAT",
            $"{_options.Zona}.BYE",
            $"{_options.Zona}.VIDEO",
            $"{_options.Zona}.TEMP",
            $"{_options.Zona}.AR",
            $"{_options.Zona}.RUIDO",
            $"{_options.Zona}.HUM"
        };

        foreach (string binding in defaultBindings)
        {
            channel.QueueBind(
                queue: _options.QueueName,
                exchange: _options.ExchangeName,
                routingKey: binding);

            Console.WriteLine($"[{_options.InstanceName}] Binding: {binding}");
        }

        EventingBasicConsumer consumer = new(channel);
        consumer.Received += (model, ea) =>
        {
            try
            {
                string json = Encoding.UTF8.GetString(ea.Body.ToArray());
                MonitoringMessage? msg = JsonSerializer.Deserialize<MonitoringMessage>(json);

                if (msg != null)
                {
                    _processor.Process(msg);
                }

                channel.BasicAck(ea.DeliveryTag, false);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_options.InstanceName}] Erro no consumo RabbitMQ: {ex.Message}");
                channel.BasicAck(ea.DeliveryTag, false);
            }
        };

        channel.BasicConsume(
            queue: _options.QueueName,
            autoAck: false,
            consumer: consumer);

        Console.WriteLine($"[{_options.InstanceName}] À escuta. Prima ENTER para terminar.");
        Console.ReadLine();
    }

    private void MonitorHeartbeats()
    {
        while (true)
        {
            Thread.Sleep(10000);

            foreach (string sensorId in _heartbeatRegistry.GetInactiveSensors())
            {
                _logger.Write($"ALERTA: Sensor {sensorId} sem heartbeat há mais de 30 segundos");
                Console.WriteLine($"[{_options.InstanceName}] ALERTA: Sensor {sensorId} inativo");
            }
        }
    }
}

public static class Program
{
    public static void Main(string[] args)
    {
        Console.Write("Nome da instância do gateway (ex: G1, G2): ");
        string instanceName = Console.ReadLine()?.Trim() ?? "G1";

        Console.Write("Zona do gateway (ZONA_ESCOLAR, ZONA_CENTRO, ZONA_INDUSTRIAL, ZONA_RESIDENCIAL): ");
        string zona = (Console.ReadLine()?.Trim() ?? "ZONA_ESCOLAR").ToUpperInvariant();

        string connectionString =
            @"Server=(localdb)\MSSQLLocalDB;Database=MonitorizacaoUrbana;Trusted_Connection=True;TrustServerCertificate=True;";

        GatewayOptions options = new(instanceName, zona);
        SensorRepository sensorRepository = new(connectionString);
        sensorRepository.EnsureTable();

        HeartbeatRegistry heartbeatRegistry = new();
        FileLoggerService logger = new(options.LogFile);
        PreprocessRpcClient preprocessRpcClient = new("127.0.0.1", 7000, logger);
        ServerForwarder serverForwarder = new("127.0.0.1", 6000, logger);

        GatewayMessageProcessor processor = new(
            options,
            sensorRepository,
            heartbeatRegistry,
            preprocessRpcClient,
            serverForwarder,
            logger);

        GatewayHost host = new(
            options,
            sensorRepository,
            processor,
            heartbeatRegistry,
            logger);

        host.Run();
    }
}