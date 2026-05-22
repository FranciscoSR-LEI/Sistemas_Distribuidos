using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;

namespace AnaliseRpcApp;

public class AnalysisRequest
{
    public string Timestamp { get; set; } = "";
    public string SensorId { get; set; } = "";
    public string Zona { get; set; } = "";
    public string Tipo { get; set; } = "";
    public double Valor { get; set; }
}

public class AnalysisResponse
{
    public bool Ok { get; set; }
    public string RiskLevel { get; set; } = "";
    public string Summary { get; set; } = "";
    public string RecommendedAction { get; set; } = "";
}

public class AnalysisService
{
    public AnalysisResponse Analyse(AnalysisRequest req)
    {
        string risk = "LOW";
        string summary = "Situação normal.";
        string action = "Sem ação imediata.";

        string tipo = req.Tipo.ToUpperInvariant();

        if (tipo == "TEMP")
        {
            if (req.Valor >= 28)
            {
                risk = "MEDIUM";
                summary = "Temperatura elevada detetada.";
                action = "Monitorizar evolução térmica.";
            }
        }
        else if (tipo == "AR")
        {
            if (req.Valor >= 70)
            {
                risk = "HIGH";
                summary = "Qualidade do ar degradada.";
                action = "Emitir alerta e reforçar monitorização.";
            }
            else if (req.Valor >= 40)
            {
                risk = "MEDIUM";
                summary = "Qualidade do ar moderada.";
                action = "Acompanhar tendência.";
            }
        }
        else if (tipo == "RUIDO")
        {
            if (req.Valor >= 85)
            {
                risk = "HIGH";
                summary = "Ruído urbano excessivo.";
                action = "Verificar fonte de ruído.";
            }
            else if (req.Valor >= 65)
            {
                risk = "MEDIUM";
                summary = "Ruído acima do desejável.";
                action = "Manter observação.";
            }
        }
        else if (tipo == "HUM")
        {
            if (req.Valor >= 80)
            {
                risk = "MEDIUM";
                summary = "Humidade elevada.";
                action = "Verificar condições ambientais.";
            }
        }

        return new AnalysisResponse
        {
            Ok = true,
            RiskLevel = risk,
            Summary = summary,
            RecommendedAction = action
        };
    }
}

public class RpcHost
{
    private readonly int _port;
    private readonly AnalysisService _service;

    public RpcHost(int port, AnalysisService service)
    {
        _port = port;
        _service = service;
    }

    public void Run()
    {
        TcpListener listener = new(IPAddress.Any, _port);
        listener.Start();

        Console.WriteLine($"[ANALISE RPC] À escuta na porta {_port}...");

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

            AnalysisRequest? request = JsonSerializer.Deserialize<AnalysisRequest>(json);
            if (request == null)
                return;

            Console.WriteLine($"[ANALISE RPC] Pedido recebido: Sensor={request.SensorId}, Tipo={request.Tipo}, Valor={request.Valor}");

            AnalysisResponse response = _service.Analyse(request);
            writer.WriteLine(JsonSerializer.Serialize(response));
        }
        catch (Exception ex)
        {
            Console.WriteLine("[ANALISE RPC] Erro: " + ex.Message);
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
        RpcHost host = new(8000, new AnalysisService());
        host.Run();
    }
}