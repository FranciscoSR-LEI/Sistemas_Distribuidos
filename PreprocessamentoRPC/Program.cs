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
    public string Valor { get; set; } = "";
}

public class PreprocessResponse
{
    public bool Ok { get; set; }
    public string NormalizedValue { get; set; } = "";
    public string Notes { get; set; } = "";
}

public class PreprocessService
{
    public PreprocessResponse Process(PreprocessRequest request)
    {
        try
        {
            string tipo = request.Tipo.Trim().ToUpperInvariant();
            string valorTexto = request.Valor.Replace(',', '.');

            if (!double.TryParse(valorTexto, NumberStyles.Float, CultureInfo.InvariantCulture, out double valor))
            {
                return new PreprocessResponse
                {
                    Ok = false,
                    Notes = "Valor inválido"
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
                Notes = "Valor uniformizado"
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

            Console.WriteLine($"[PREPROCESSAMENTO RPC] Pedido recebido: Tipo={request.Tipo}, Valor={request.Valor}");

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