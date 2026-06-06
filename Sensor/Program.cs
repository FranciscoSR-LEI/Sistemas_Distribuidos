using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using RabbitMQ.Client;

namespace SensorApp;

public class MonitoringMessage
{
    public string MessageType { get; set; } = "";
    public string Timestamp { get; set; } = "";
    public string SensorId { get; set; } = "";
    public string Zona { get; set; } = "";
    public string Tipo { get; set; } = "";

    // legado / compatibilidade
    public string Valor { get; set; } = "";

    // novo
    public string DataFormat { get; set; } = "";
    public string RawPayload { get; set; } = "";

    public string FileName { get; set; } = "";
    public string FileContentBase64 { get; set; } = "";
}

public class SensorProfile
{
    public string SensorId { get; }
    public string Zona { get; }
    public IReadOnlyList<string> Tipos { get; }
    public string? AutomaticType { get; }

    public SensorProfile(string sensorId, string zona, IReadOnlyList<string> tipos, string? automaticType)
    {
        SensorId = sensorId;
        Zona = zona;
        Tipos = tipos;
        AutomaticType = automaticType;
    }
}

public static class SensorProfileFactory
{
    public static SensorProfile Create(int sensorNumber)
    {
        string sensorId = sensorNumber.ToString();

        if (sensorNumber >= 0 && sensorNumber <= 10)
            return new SensorProfile(sensorId, "ZONA_ESCOLAR", new List<string> { "TEMP" }, "TEMP");

        if (sensorNumber >= 11 && sensorNumber <= 20)
            return new SensorProfile(sensorId, "ZONA_CENTRO", new List<string> { "AR" }, "AR");

        if (sensorNumber >= 21 && sensorNumber <= 30)
            return new SensorProfile(sensorId, "ZONA_INDUSTRIAL", new List<string> { "RUIDO" }, "RUIDO");

        if (sensorNumber >= 31 && sensorNumber <= 40)
            return new SensorProfile(sensorId, "ZONA_RESIDENCIAL", new List<string> { "HUM" }, "HUM");

        return new SensorProfile(sensorId, "ZONA_ESCOLAR", new List<string> { "VIDEO" }, null);
    }
}

public class SensorValueGenerator
{
    private readonly Random _random = new();

    public string Generate(string tipo)
    {
        lock (_random)
        {
            return tipo.ToUpperInvariant() switch
            {
                "TEMP" => (20 + _random.NextDouble() * 10).ToString("F2", CultureInfo.InvariantCulture),
                "AR" => _random.Next(10, 101).ToString(CultureInfo.InvariantCulture),
                "RUIDO" => _random.Next(30, 101).ToString(CultureInfo.InvariantCulture),
                "HUM" => _random.Next(30, 91).ToString(CultureInfo.InvariantCulture),
                _ => "0"
            };
        }
    }

    public string PickFormat()
    {
        lock (_random)
        {
            string[] formats = { "PLAIN", "JSON", "CSV", "KV" };
            return formats[_random.Next(formats.Length)];
        }
    }
}

public class RabbitPublisher : IDisposable
{
    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly string _exchangeName;
    private readonly object _publishLock = new();

    public RabbitPublisher(string host, string user, string password, string exchangeName)
    {
        _exchangeName = exchangeName;

        var factory = new ConnectionFactory
        {
            HostName = host,
            UserName = user,
            Password = password
        };

        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();

        _channel.ExchangeDeclare(
            exchange: _exchangeName,
            type: ExchangeType.Topic,
            durable: false,
            autoDelete: false,
            arguments: null);
    }

    public void Publish(string routingKey, MonitoringMessage message)
    {
        lock (_publishLock)
        {
            string json = JsonSerializer.Serialize(message);
            byte[] body = Encoding.UTF8.GetBytes(json);

            IBasicProperties props = _channel.CreateBasicProperties();
            props.Persistent = false;

            _channel.BasicPublish(
                exchange: _exchangeName,
                routingKey: routingKey,
                basicProperties: props,
                body: body);
        }
    }

    public void Dispose()
    {
        try { _channel.Close(); } catch { }
        try { _connection.Close(); } catch { }
    }
}

public class SensorApplication
{
    private readonly SensorProfile _profile;
    private readonly RabbitPublisher _publisher;
    private readonly SensorValueGenerator _valueGenerator;
    private readonly string _videoPath;

    private volatile bool _running = true;
    private Thread? _heartbeatThread;
    private Thread? _automaticDataThread;

    public SensorApplication(
        SensorProfile profile,
        RabbitPublisher publisher,
        SensorValueGenerator valueGenerator,
        string videoPath)
    {
        _profile = profile;
        _publisher = publisher;
        _valueGenerator = valueGenerator;
        _videoPath = videoPath;
    }

    public void Run()
    {
        Console.CancelKeyPress += OnCancelKeyPress;

        PublishHello();
        StartHeartbeatLoop();

        if (_profile.AutomaticType != null)
        {
            StartAutomaticDataLoop(_profile.AutomaticType);

            Console.WriteLine($"[SENSOR] Sensor {_profile.SensorId} criado na zona {_profile.Zona}");
            Console.WriteLine($"[SENSOR] Modo automático ativo para tipo: {_profile.AutomaticType}");
            Console.WriteLine("[SENSOR] Termine com Ctrl+C.");

            while (_running)
            {
                Thread.Sleep(1000);
            }
        }
        else
        {
            Console.WriteLine($"[SENSOR] Sensor {_profile.SensorId} criado na zona {_profile.Zona}");
            Console.WriteLine("[SENSOR] Sensor de vídeo.");
            RunVideoMenu();
        }
    }

    private void StartHeartbeatLoop()
    {
        _heartbeatThread = new Thread(() =>
        {
            while (_running)
            {
                try
                {
                    Thread.Sleep(15000);
                    if (!_running) break;
                    PublishHeartbeat();
                }
                catch
                {
                    _running = false;
                    break;
                }
            }
        })
        {
            IsBackground = true
        };

        _heartbeatThread.Start();
    }

    private void StartAutomaticDataLoop(string tipo)
    {
        _automaticDataThread = new Thread(() =>
        {
            while (_running)
            {
                try
                {
                    Thread.Sleep(10000);
                    if (!_running) break;

                    string valor = _valueGenerator.Generate(tipo);
                    string dataFormat = _valueGenerator.PickFormat();
                    string rawPayload = BuildPayload(tipo, valor, dataFormat);

                    MonitoringMessage message = new()
                    {
                        MessageType = "DATA",
                        Timestamp = DateTime.Now.ToString("s"),
                        SensorId = _profile.SensorId,
                        Zona = _profile.Zona,
                        Tipo = tipo,
                        Valor = valor,
                        DataFormat = dataFormat,
                        RawPayload = rawPayload
                    };

                    _publisher.Publish($"{_profile.Zona}.{tipo}", message);

                    Console.WriteLine($"[SENSOR] DATA publicado: zona={_profile.Zona}, tipo={tipo}, formato={dataFormat}, payload={rawPayload}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine("[SENSOR] Erro no envio automático: " + ex.Message);
                    _running = false;
                    break;
                }
            }
        })
        {
            IsBackground = true
        };

        _automaticDataThread.Start();
    }

    private string BuildPayload(string tipo, string valor, string format)
    {
        string timestamp = DateTime.Now.ToString("s");

        return format.ToUpperInvariant() switch
        {
            "PLAIN" => valor,
            "JSON" => JsonSerializer.Serialize(new
            {
                sensorId = _profile.SensorId,
                zona = _profile.Zona,
                tipo = tipo,
                valor = valor,
                timestamp = timestamp
            }),
            "CSV" => $"{_profile.SensorId};{_profile.Zona};{tipo};{valor};{timestamp}",
            "KV" => $"sensor={_profile.SensorId}|zona={_profile.Zona}|tipo={tipo}|valor={valor}|timestamp={timestamp}",
            _ => valor
        };
    }

    private void RunVideoMenu()
    {
        while (_running)
        {
            Console.WriteLine();
            Console.WriteLine("--- MENU SENSOR VIDEO ---");
            Console.WriteLine("1 - Publicar vídeo");
            Console.WriteLine("2 - Publicar heartbeat manual");
            Console.WriteLine("3 - Terminar");
            Console.Write("Escolha: ");

            string? option = Console.ReadLine();

            switch (option)
            {
                case "1":
                    PublishVideo();
                    break;
                case "2":
                    PublishHeartbeat();
                    break;
                case "3":
                    Terminate();
                    break;
                default:
                    Console.WriteLine("Opção inválida.");
                    break;
            }
        }
    }

    private void PublishHello()
    {
        MonitoringMessage message = new()
        {
            MessageType = "HELLO",
            Timestamp = DateTime.Now.ToString("s"),
            SensorId = _profile.SensorId,
            Zona = _profile.Zona,
            Tipo = string.Join(",", _profile.Tipos)
        };

        _publisher.Publish($"{_profile.Zona}.HELLO", message);
        Console.WriteLine("[SENSOR] HELLO publicado.");
    }

    private void PublishHeartbeat()
    {
        MonitoringMessage message = new()
        {
            MessageType = "HEARTBEAT",
            Timestamp = DateTime.Now.ToString("s"),
            SensorId = _profile.SensorId,
            Zona = _profile.Zona
        };

        _publisher.Publish($"{_profile.Zona}.HEARTBEAT", message);
        Console.WriteLine("[SENSOR] HEARTBEAT publicado.");
    }

    private void PublishVideo()
    {
        try
        {
            if (!File.Exists(_videoPath))
            {
                Console.WriteLine("[SENSOR] Ficheiro de vídeo não encontrado: " + _videoPath);
                return;
            }

            byte[] bytes = File.ReadAllBytes(_videoPath);

            MonitoringMessage message = new()
            {
                MessageType = "VIDEO",
                Timestamp = DateTime.Now.ToString("s"),
                SensorId = _profile.SensorId,
                Zona = _profile.Zona,
                Tipo = "VIDEO",
                FileName = Path.GetFileName(_videoPath),
                FileContentBase64 = Convert.ToBase64String(bytes)
            };

            _publisher.Publish($"{_profile.Zona}.VIDEO", message);
            Console.WriteLine($"[SENSOR] VIDEO publicado: zona={_profile.Zona}, ficheiro={message.FileName}, bytes={bytes.Length}");
        }
        catch (Exception ex)
        {
            Console.WriteLine("[SENSOR] Erro ao publicar vídeo: " + ex.Message);
        }
    }

    private void PublishBye()
    {
        MonitoringMessage message = new()
        {
            MessageType = "BYE",
            Timestamp = DateTime.Now.ToString("s"),
            SensorId = _profile.SensorId,
            Zona = _profile.Zona
        };

        _publisher.Publish($"{_profile.Zona}.BYE", message);
        Console.WriteLine("[SENSOR] BYE publicado.");
    }

    private void OnCancelKeyPress(object? sender, ConsoleCancelEventArgs e)
    {
        e.Cancel = true;
        Console.WriteLine("\n[SENSOR] A terminar...");
        Terminate();
    }

    private void Terminate()
    {
        try
        {
            PublishBye();
        }
        catch
        {
        }
        finally
        {
            _running = false;
        }
    }
}

public static class Program
{
    public static void Main(string[] args)
    {
        Console.Write("Introduza o sensorId (0 a 50): ");
        string? input = Console.ReadLine()?.Trim();

        if (!int.TryParse(input, out int sensorNumber) || sensorNumber < 0 || sensorNumber > 50)
        {
            Console.WriteLine("[SENSOR] sensorId inválido.");
            return;
        }

        SensorProfile profile = SensorProfileFactory.Create(sensorNumber);
        SensorValueGenerator valueGenerator = new();
        string videoPath = @"C:\Users\vitor\source\repos\Trabalho_SD_fianl2\Sensor\Video_demo.mp4";

        using RabbitPublisher publisher = new("localhost", "guest", "guest", "urban.topic");
        SensorApplication app = new(profile, publisher, valueGenerator, videoPath);
        app.Run();
    }
}