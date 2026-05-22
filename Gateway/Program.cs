using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
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
    public string FileName { get; set; } = "";
    public string FileContentBase64 { get; set; } = "";
}

public class SensorConfig
{
    public string SensorId { get; set; } = "";
    public string Estado { get; set; } = "";
    public string Zona { get; set; } = "";
    public List<string> Tipos { get; set; } = new();
    public string LastSync { get; set; } = "";
}

public class PreprocessRequest
{
    public string Tipo { get; set; } = "";
    public string Valor { get; set; } = "";
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
    public string ConfigFile { get; }
    public string LogFile { get; }

    public GatewayOptions(string instanceName, string zona, string configFile)
    {
        InstanceName = instanceName;
        Zona = zona;
        ExchangeName = "urban.topic";
        QueueName = $"queue.{zona.ToLowerInvariant()}";
        ConfigFile = configFile;
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

public class GatewayConfigRepository
{
    private readonly string _configFile;
    private readonly object _lockObj = new();

    public GatewayConfigRepository(string configFile)
    {
        _configFile = configFile;
    }

    public SensorConfig? GetById(string sensorId)
    {
        lock (_lockObj)
        {
            if (!File.Exists(_configFile))
                return null;

            foreach (string line in File.ReadAllLines(_configFile))
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                string[] parts = line.Split(':');
                if (parts.Length < 5)
                    continue;

                if (!parts[0].Trim().Equals(sensorId, StringComparison.OrdinalIgnoreCase))
                    continue;

                string tiposPart = parts[3].Trim().Replace("[", "").Replace("]", "");
                List<string> tipos = new();

                foreach (string tipo in tiposPart.Split(','))
                {
                    if (!string.IsNullOrWhiteSpace(tipo))
                        tipos.Add(tipo.Trim());
                }

                return new SensorConfig
                {
                    SensorId = parts[0].Trim(),
                    Estado = parts[1].Trim(),
                    Zona = parts[2].Trim(),
                    Tipos = tipos,
                    LastSync = parts[4].Trim()
                };
            }

            return null;
        }
    }

    public void UpdateLastSync(string sensorId)
    {
        lock (_lockObj)
        {
            if (!File.Exists(_configFile))
                return;

            string[] lines = File.ReadAllLines(_configFile);

            for (int i = 0; i < lines.Length; i++)
            {
                if (string.IsNullOrWhiteSpace(lines[i]))
                    continue;

                string[] parts = lines[i].Split(':');
                if (parts.Length < 5)
                    continue;

                if (!parts[0].Trim().Equals(sensorId, StringComparison.OrdinalIgnoreCase))
                    continue;

                parts[4] = DateTime.Now.ToString("s");
                lines[i] = string.Join(":", parts);
                break;
            }

            File.WriteAllLines(_configFile, lines);
        }
    }

    public IEnumerable<string> GetBindingsForZone(string zona)
    {
        HashSet<string> bindings = new(StringComparer.OrdinalIgnoreCase);

        lock (_lockObj)
        {
            if (!File.Exists(_configFile))
                return bindings;

            foreach (string line in File.ReadAllLines(_configFile))
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                string[] parts = line.Split(':');
                if (parts.Length < 5)
                    continue;

                string estado = parts[1].Trim();
                string zonaConfig = parts[2].Trim();

                if (!estado.Equals("ativo", StringComparison.OrdinalIgnoreCase))
                    continue;

                if (!zonaConfig.Equals(zona, StringComparison.OrdinalIgnoreCase))
                    continue;

                string tiposPart = parts[3].Trim().Replace("[", "").Replace("]", "");
                string[] tipos = tiposPart.Split(',');

                bindings.Add($"{zona}.HELLO");
                bindings.Add($"{zona}.HEARTBEAT");
                bindings.Add($"{zona}.BYE");
                bindings.Add($"{zona}.VIDEO");

                foreach (string tipo in tipos)
                {
                    if (!string.IsNullOrWhiteSpace(tipo))
                        bindings.Add($"{zona}.{tipo.Trim()}");
                }
            }
        }

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

    public PreprocessResponse Normalize(string tipo, string valor)
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
                Valor = valor
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
    private readonly GatewayConfigRepository _configRepository;
    private readonly HeartbeatRegistry _heartbeatRegistry;
    private readonly PreprocessRpcClient _preprocessRpcClient;
    private readonly ServerForwarder _serverForwarder;
    private readonly ILoggerService _logger;

    public GatewayMessageProcessor(
        GatewayOptions options,
        GatewayConfigRepository configRepository,
        HeartbeatRegistry heartbeatRegistry,
        PreprocessRpcClient preprocessRpcClient,
        ServerForwarder serverForwarder,
        ILoggerService logger)
    {
        _options = options;
        _configRepository = configRepository;
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
                _logger.Write($"BYE de {msg.SensorId}");
                Console.WriteLine($"[{_options.InstanceName}] Sensor {msg.SensorId} terminou.");
                break;
        }
    }

    private void ProcessHello(MonitoringMessage msg)
    {
        SensorConfig? config = _configRepository.GetById(msg.SensorId);
        if (config == null)
        {
            _logger.Write($"HELLO rejeitado: sensor {msg.SensorId} não registado");
            return;
        }

        if (!IsActive(config))
        {
            _logger.Write($"HELLO rejeitado: sensor {msg.SensorId} indisponível");
            return;
        }

        if (!config.Zona.Equals(msg.Zona, StringComparison.OrdinalIgnoreCase))
        {
            _logger.Write($"HELLO rejeitado: zona inválida de {msg.SensorId}");
            return;
        }

        _configRepository.UpdateLastSync(msg.SensorId);
        _heartbeatRegistry.Update(msg.SensorId);

        _logger.Write($"HELLO aceite de {msg.SensorId} na zona {msg.Zona}");
    }

    private void ProcessHeartbeat(MonitoringMessage msg)
    {
        SensorConfig? config = _configRepository.GetById(msg.SensorId);
        if (config == null)
            return;

        _configRepository.UpdateLastSync(msg.SensorId);
        _heartbeatRegistry.Update(msg.SensorId);
        _logger.Write($"HEARTBEAT de {msg.SensorId}");
    }

    private void ProcessData(MonitoringMessage msg)
    {
        SensorConfig? config = _configRepository.GetById(msg.SensorId);
        if (config == null)
        {
            _logger.Write($"DATA rejeitado: sensor {msg.SensorId} não registado");
            return;
        }

        if (!IsActive(config))
        {
            _logger.Write($"DATA rejeitado: sensor {msg.SensorId} indisponível");
            return;
        }

        if (!SupportsType(config, msg.Tipo))
        {
            _logger.Write($"DATA rejeitado: tipo {msg.Tipo} não suportado por {msg.SensorId}");
            return;
        }

        PreprocessResponse response = _preprocessRpcClient.Normalize(msg.Tipo, msg.Valor);
        if (!response.Ok)
        {
            _logger.Write($"Pré-processamento falhou para {msg.SensorId}");
            return;
        }

        _configRepository.UpdateLastSync(msg.SensorId);
        _heartbeatRegistry.Update(msg.SensorId);

        bool ok = _serverForwarder.SendStore(
            msg.Timestamp,
            msg.SensorId,
            msg.Zona,
            msg.Tipo,
            response.NormalizedValue,
            _options.InstanceName);

        _logger.Write(ok
            ? $"DATA encaminhado para servidor: gateway={_options.InstanceName}, sensor={msg.SensorId}, {msg.Tipo}={response.NormalizedValue}"
            : $"Falha ao encaminhar DATA de {msg.SensorId}");
    }

    private void ProcessVideo(MonitoringMessage msg)
    {
        SensorConfig? config = _configRepository.GetById(msg.SensorId);
        if (config == null)
        {
            _logger.Write($"VIDEO rejeitado: sensor {msg.SensorId} não registado");
            return;
        }

        if (!IsActive(config))
        {
            _logger.Write($"VIDEO rejeitado: sensor {msg.SensorId} indisponível");
            return;
        }

        if (!SupportsType(config, "VIDEO"))
        {
            _logger.Write($"VIDEO rejeitado: sensor {msg.SensorId} não suporta VIDEO");
            return;
        }

        byte[] videoBytes = Convert.FromBase64String(msg.FileContentBase64);

        _configRepository.UpdateLastSync(msg.SensorId);
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

    private static bool IsActive(SensorConfig config)
    {
        if (config.Estado.Equals("desativado", StringComparison.OrdinalIgnoreCase))
            return false;

        if (config.Estado.Equals("manutencao", StringComparison.OrdinalIgnoreCase) ||
            config.Estado.Equals("manutenção", StringComparison.OrdinalIgnoreCase))
            return false;

        return config.Estado.Equals("ativo", StringComparison.OrdinalIgnoreCase);
    }

    private static bool SupportsType(SensorConfig config, string tipo)
    {
        foreach (string t in config.Tipos)
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
    private readonly GatewayConfigRepository _configRepository;
    private readonly GatewayMessageProcessor _processor;
    private readonly HeartbeatRegistry _heartbeatRegistry;
    private readonly ILoggerService _logger;

    public GatewayHost(
        GatewayOptions options,
        GatewayConfigRepository configRepository,
        GatewayMessageProcessor processor,
        HeartbeatRegistry heartbeatRegistry,
        ILoggerService logger)
    {
        _options = options;
        _configRepository = configRepository;
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
            durable: false,
            autoDelete: false);

        channel.QueueDeclare(
            queue: _options.QueueName,
            durable: false,
            exclusive: false,
            autoDelete: false);

        channel.BasicQos(0, 1, false);

        foreach (string binding in _configRepository.GetBindingsForZone(_options.Zona))
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
                Console.WriteLine($"[{_options.InstanceName}] Erro no consumo RabbitMQ: " + ex.Message);
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

        string configFile = @"C:\Users\vitor\source\repos\Trabalho_SD_fianl2\Gateway\sensores_config.csv";

        GatewayOptions options = new(instanceName, zona, configFile);
        GatewayConfigRepository configRepository = new(options.ConfigFile);
        HeartbeatRegistry heartbeatRegistry = new();
        FileLoggerService logger = new(options.LogFile);
        PreprocessRpcClient preprocessRpcClient = new("127.0.0.1", 7000, logger);
        ServerForwarder serverForwarder = new("127.0.0.1", 6000, logger);

        GatewayMessageProcessor processor = new(
            options,
            configRepository,
            heartbeatRegistry,
            preprocessRpcClient,
            serverForwarder,
            logger);

        GatewayHost host = new(
            options,
            configRepository,
            processor,
            heartbeatRegistry,
            logger);

        host.Run();
    }
}