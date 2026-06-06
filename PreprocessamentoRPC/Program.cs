using System;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;

namespace PreprocessamentoRpcApp;

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

public class JsonPayload
{
    public string sensorId { get; set; } = "";
    public string zona { get; set; } = "";
    public string tipo { get; set; } = "";
    public string valor { get; set; } = "";
    public string timestamp { get; set; } = "";
}

public class PreprocessService
{
    public PreprocessResponse Process(PreprocessRequest request)
    {
        try
        {
            string tipo = request.Tipo.Trim().ToUpperInvariant();
            string format = request.DataFormat.Trim().ToUpperInvariant();
            string raw = request.RawPayload.Trim();

            string? extractedValue = ExtractValue(format, raw);

            if (string.IsNullOrWhiteSpace(extractedValue))
            {
                return new PreprocessResponse
                {
                    Ok = false,
                    Notes = $"Não foi possível extrair valor do formato {format}"
                };
            }

            string valorTexto = extractedValue.Replace(',', '.');

            if (!double.TryParse(valorTexto, NumberStyles.Float, CultureInfo.InvariantCulture, out double valor))
            {
                return new PreprocessResponse
                {
                    Ok = false,
                    Notes = $"Valor inválido após extração: {extractedValue}"
                };
            }

            double normalized = tipo switch
            {
                "TEMP" => Math.Round(valor, 2),
                "AR" => Math.Round(valor, 0),
                "RUIDO" => Math.Round(valor, 0),
                "HUM" => Math.Round(valor, 0),
                _ => valor
            };

            return new PreprocessResponse
            {
                Ok = true,
                NormalizedValue = normalized.ToString(CultureInfo.InvariantCulture),
                Notes = $"Valor uniformizado a partir do formato {format}"
            };
        }
        catch (Exception ex)
        {
            return new PreprocessResponse
            {
                Ok = false,
                Notes = ex.Message
            };
        }
    }

    private string? ExtractValue(string format, string raw)
    {
        return format switch
        {
            "PLAIN" => ExtractPlain(raw),
            "JSON" => ExtractJson(raw),
            "CSV" => ExtractCsv(raw),
            "KV" => ExtractKv(raw),
            _ => null
        };
    }

    private string? ExtractPlain(string raw)
    {
        return raw;
    }

    private string? ExtractJson(string raw)
    {
        JsonPayload? payload = JsonSerializer.Deserialize<JsonPayload>(raw);
        return payload?.valor;
    }

    private string? ExtractCsv(string raw)
    {
        // formato: sensorId;zona;tipo;valor;timestamp
        string[] parts = raw.Split(';');
        if (parts.Length < 5)
            return null;

        return parts[3].Trim();
    }

    private string? ExtractKv(string raw)
    {
        // formato: sensor=...|zona=...|tipo=...|valor=...|timestamp=...
        string[] parts = raw.Split('|');

        foreach (string part in parts)
        {
            string[] kv = part.Split('=', 2);
            if (kv.Length == 2 && kv[0].Trim().Equals("valor", StringComparison.OrdinalIgnoreCase))
                return kv[1].Trim();
        }

        return null;
    }
}

public class RpcHost
{
    private readonly int _port;
    private readonly PreprocessService _service;

    public RpcHost(int port, PreprocessService service)
    {
        _port = port;
        _service = service;
    }

    public void Run()
    {
        TcpListener listener = new(IPAddress.Any, _port);
        listener.Start();

        Console.WriteLine($"[PREPROCESSAMENTO RPC] À escuta na porta {_port}...");

        while (true)
        {
            TcpClient client = listener.AcceptTcpClient();
            Thread thread = new(() => HandleClient(client))
            {
                IsBackground = true
            };
            thread.Start();
        }
    }

    private void HandleClient(TcpClient client)
    {
        try
        {
            using NetworkStream ns = client.GetStream();
            using StreamReader reader = new(ns);
            using StreamWriter writer = new(ns) { AutoFlush = true };

            string? json = reader.ReadLine();
            if (json == null)
                return;

            PreprocessRequest? request = JsonSerializer.Deserialize<PreprocessRequest>(json);
            if (request == null)
                return;

            Console.WriteLine($"[PREPROCESSAMENTO RPC] Pedido recebido: Tipo={request.Tipo}, Formato={request.DataFormat}, Payload={request.RawPayload}");

            PreprocessResponse response = _service.Process(request);
            writer.WriteLine(JsonSerializer.Serialize(response));
        }
        catch (Exception ex)
        {
            Console.WriteLine("[PREPROCESSAMENTO RPC] Erro: " + ex.Message);
        }
        finally
        {
            try { client.Close(); } catch { }
        }
    }
}

public static class Program
{
    public static void Main(string[] args)
    {
        RpcHost host = new(7000, new PreprocessService());
        host.Run();
    }
}