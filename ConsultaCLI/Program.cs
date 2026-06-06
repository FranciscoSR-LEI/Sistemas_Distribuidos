using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace ConsultaCliApp;

public class MeasurementDto
{
    public int Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string SensorId { get; set; } = "";
    public string Zona { get; set; } = "";
    public string Tipo { get; set; } = "";
    public double Valor { get; set; }
    public string ProcessedByGateway { get; set; } = "";
}

public class VideoEventDto
{
    public int Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string SensorId { get; set; } = "";
    public string Zona { get; set; } = "";
    public string FileName { get; set; } = "";
    public string FilePath { get; set; } = "";
    public long FileSize { get; set; }
    public string ProcessedByGateway { get; set; } = "";
}

public class AnalysisResultDto
{
    public int Id { get; set; }
    public int? MeasurementId { get; set; }
    public DateTime Timestamp { get; set; }
    public string SensorId { get; set; } = "";
    public string Zona { get; set; } = "";
    public string Tipo { get; set; } = "";
    public double Valor { get; set; }
    public string RiskLevel { get; set; } = "";
    public string Summary { get; set; } = "";
    public string RecommendedAction { get; set; } = "";
    public string ProcessedByGateway { get; set; } = "";
}

public class MeasurementStatsDto
{
    public string Tipo { get; set; } = "";
    public int TotalRegistos { get; set; }
    public double ValorMedio { get; set; }
    public double ValorMinimo { get; set; }
    public double ValorMaximo { get; set; }
}

public class DashboardStatsDto
{
    public int TotalMeasurements { get; set; }
    public int TotalVideos { get; set; }
    public int TotalAnalyses { get; set; }
    public int LowCount { get; set; }
    public int MediumCount { get; set; }
    public int HighCount { get; set; }
}

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

public class DatabaseRepository
{
    private readonly string _connectionString;

    public DatabaseRepository(string connectionString)
    {
        _connectionString = connectionString;
    }

    public DashboardStatsDto GetDashboardStats()
    {
        DashboardStatsDto stats = new();

        using SqlConnection connection = new(_connectionString);
        connection.Open();

        using (SqlCommand cmd = new("SELECT COUNT(*) FROM Measurements;", connection))
        {
            stats.TotalMeasurements = Convert.ToInt32(cmd.ExecuteScalar());
        }

        using (SqlCommand cmd = new("SELECT COUNT(*) FROM VideoEvents;", connection))
        {
            stats.TotalVideos = Convert.ToInt32(cmd.ExecuteScalar());
        }

        using (SqlCommand cmd = new("SELECT COUNT(*) FROM AnalysisResults;", connection))
        {
            stats.TotalAnalyses = Convert.ToInt32(cmd.ExecuteScalar());
        }

        using (SqlCommand cmd = new(@"
SELECT
    SUM(CASE WHEN UPPER(RiskLevel) = 'LOW' THEN 1 ELSE 0 END),
    SUM(CASE WHEN UPPER(RiskLevel) = 'MEDIUM' THEN 1 ELSE 0 END),
    SUM(CASE WHEN UPPER(RiskLevel) = 'HIGH' THEN 1 ELSE 0 END)
FROM AnalysisResults;", connection))
        using (SqlDataReader reader = cmd.ExecuteReader())
        {
            if (reader.Read())
            {
                stats.LowCount = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0));
                stats.MediumCount = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1));
                stats.HighCount = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2));
            }
        }

        return stats;
    }

    public List<MeasurementDto> GetLatestMeasurements(int top)
    {
        List<MeasurementDto> items = new();

        using SqlConnection connection = new(_connectionString);
        connection.Open();

        string sql = @"
SELECT TOP (@Top)
    Id, [Timestamp], SensorId, Zona, Tipo, Valor, ProcessedByGateway
FROM Measurements
ORDER BY [Timestamp] DESC;";

        using SqlCommand cmd = new(sql, connection);
        cmd.Parameters.AddWithValue("@Top", top);

        using SqlDataReader reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            items.Add(new MeasurementDto
            {
                Id = reader.GetInt32(0),
                Timestamp = reader.GetDateTime(1),
                SensorId = reader.GetString(2),
                Zona = reader.GetString(3),
                Tipo = reader.GetString(4),
                Valor = reader.GetDouble(5),
                ProcessedByGateway = reader.IsDBNull(6) ? "" : reader.GetString(6)
            });
        }

        return items;
    }

    public List<MeasurementDto> GetMeasurementsBySensor(string sensorId, int top)
    {
        List<MeasurementDto> items = new();

        using SqlConnection connection = new(_connectionString);
        connection.Open();

        string sql = @"
SELECT TOP (@Top)
    Id, [Timestamp], SensorId, Zona, Tipo, Valor, ProcessedByGateway
FROM Measurements
WHERE SensorId = @SensorId
ORDER BY [Timestamp] DESC;";

        using SqlCommand cmd = new(sql, connection);
        cmd.Parameters.AddWithValue("@Top", top);
        cmd.Parameters.AddWithValue("@SensorId", sensorId);

        using SqlDataReader reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            items.Add(new MeasurementDto
            {
                Id = reader.GetInt32(0),
                Timestamp = reader.GetDateTime(1),
                SensorId = reader.GetString(2),
                Zona = reader.GetString(3),
                Tipo = reader.GetString(4),
                Valor = reader.GetDouble(5),
                ProcessedByGateway = reader.IsDBNull(6) ? "" : reader.GetString(6)
            });
        }

        return items;
    }

    public List<MeasurementDto> GetMeasurementsByZone(string zona, int top)
    {
        List<MeasurementDto> items = new();

        using SqlConnection connection = new(_connectionString);
        connection.Open();

        string sql = @"
SELECT TOP (@Top)
    Id, [Timestamp], SensorId, Zona, Tipo, Valor, ProcessedByGateway
FROM Measurements
WHERE UPPER(Zona) = UPPER(@Zona)
ORDER BY [Timestamp] DESC;";

        using SqlCommand cmd = new(sql, connection);
        cmd.Parameters.AddWithValue("@Top", top);
        cmd.Parameters.AddWithValue("@Zona", zona);

        using SqlDataReader reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            items.Add(new MeasurementDto
            {
                Id = reader.GetInt32(0),
                Timestamp = reader.GetDateTime(1),
                SensorId = reader.GetString(2),
                Zona = reader.GetString(3),
                Tipo = reader.GetString(4),
                Valor = reader.GetDouble(5),
                ProcessedByGateway = reader.IsDBNull(6) ? "" : reader.GetString(6)
            });
        }

        return items;
    }

    public List<MeasurementDto> GetMeasurementsByGateway(string gateway, int top)
    {
        List<MeasurementDto> items = new();

        using SqlConnection connection = new(_connectionString);
        connection.Open();

        string sql = @"
SELECT TOP (@Top)
    Id, [Timestamp], SensorId, Zona, Tipo, Valor, ProcessedByGateway
FROM Measurements
WHERE UPPER(ProcessedByGateway) = UPPER(@Gateway)
ORDER BY [Timestamp] DESC;";

        using SqlCommand cmd = new(sql, connection);
        cmd.Parameters.AddWithValue("@Top", top);
        cmd.Parameters.AddWithValue("@Gateway", gateway);

        using SqlDataReader reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            items.Add(new MeasurementDto
            {
                Id = reader.GetInt32(0),
                Timestamp = reader.GetDateTime(1),
                SensorId = reader.GetString(2),
                Zona = reader.GetString(3),
                Tipo = reader.GetString(4),
                Valor = reader.GetDouble(5),
                ProcessedByGateway = reader.IsDBNull(6) ? "" : reader.GetString(6)
            });
        }

        return items;
    }

    public List<VideoEventDto> GetLatestVideos(int top)
    {
        List<VideoEventDto> items = new();

        using SqlConnection connection = new(_connectionString);
        connection.Open();

        string sql = @"
SELECT TOP (@Top)
    Id, [Timestamp], SensorId, Zona, FileName, FilePath, FileSize, ProcessedByGateway
FROM VideoEvents
ORDER BY [Timestamp] DESC;";

        using SqlCommand cmd = new(sql, connection);
        cmd.Parameters.AddWithValue("@Top", top);

        using SqlDataReader reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            items.Add(new VideoEventDto
            {
                Id = reader.GetInt32(0),
                Timestamp = reader.GetDateTime(1),
                SensorId = reader.GetString(2),
                Zona = reader.GetString(3),
                FileName = reader.GetString(4),
                FilePath = reader.GetString(5),
                FileSize = reader.GetInt64(6),
                ProcessedByGateway = reader.IsDBNull(7) ? "" : reader.GetString(7)
            });
        }

        return items;
    }

    public VideoEventDto? GetVideoById(int id)
    {
        using SqlConnection connection = new(_connectionString);
        connection.Open();

        string sql = @"
SELECT
    Id, [Timestamp], SensorId, Zona, FileName, FilePath, FileSize, ProcessedByGateway
FROM VideoEvents
WHERE Id = @Id;";

        using SqlCommand cmd = new(sql, connection);
        cmd.Parameters.AddWithValue("@Id", id);

        using SqlDataReader reader = cmd.ExecuteReader();
        if (reader.Read())
        {
            return new VideoEventDto
            {
                Id = reader.GetInt32(0),
                Timestamp = reader.GetDateTime(1),
                SensorId = reader.GetString(2),
                Zona = reader.GetString(3),
                FileName = reader.GetString(4),
                FilePath = reader.GetString(5),
                FileSize = reader.GetInt64(6),
                ProcessedByGateway = reader.IsDBNull(7) ? "" : reader.GetString(7)
            };
        }

        return null;
    }

    public List<AnalysisResultDto> GetLatestAnalyses(int top)
    {
        List<AnalysisResultDto> items = new();

        using SqlConnection connection = new(_connectionString);
        connection.Open();

        string sql = @"
SELECT TOP (@Top)
    Id, MeasurementId, [Timestamp], SensorId, Zona, Tipo, Valor,
    RiskLevel, Summary, RecommendedAction, ProcessedByGateway
FROM AnalysisResults
ORDER BY [Timestamp] DESC;";

        using SqlCommand cmd = new(sql, connection);
        cmd.Parameters.AddWithValue("@Top", top);

        using SqlDataReader reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            items.Add(new AnalysisResultDto
            {
                Id = reader.GetInt32(0),
                MeasurementId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                Timestamp = reader.GetDateTime(2),
                SensorId = reader.GetString(3),
                Zona = reader.GetString(4),
                Tipo = reader.GetString(5),
                Valor = reader.GetDouble(6),
                RiskLevel = reader.GetString(7),
                Summary = reader.GetString(8),
                RecommendedAction = reader.GetString(9),
                ProcessedByGateway = reader.IsDBNull(10) ? "" : reader.GetString(10)
            });
        }

        return items;
    }

    public List<AnalysisResultDto> GetAnalysesByRisk(string riskLevel)
    {
        List<AnalysisResultDto> items = new();

        using SqlConnection connection = new(_connectionString);
        connection.Open();

        string sql = @"
SELECT
    Id, MeasurementId, [Timestamp], SensorId, Zona, Tipo, Valor,
    RiskLevel, Summary, RecommendedAction, ProcessedByGateway
FROM AnalysisResults
WHERE UPPER(RiskLevel) = UPPER(@RiskLevel)
ORDER BY [Timestamp] DESC;";

        using SqlCommand cmd = new(sql, connection);
        cmd.Parameters.AddWithValue("@RiskLevel", riskLevel);

        using SqlDataReader reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            items.Add(new AnalysisResultDto
            {
                Id = reader.GetInt32(0),
                MeasurementId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                Timestamp = reader.GetDateTime(2),
                SensorId = reader.GetString(3),
                Zona = reader.GetString(4),
                Tipo = reader.GetString(5),
                Valor = reader.GetDouble(6),
                RiskLevel = reader.GetString(7),
                Summary = reader.GetString(8),
                RecommendedAction = reader.GetString(9),
                ProcessedByGateway = reader.IsDBNull(10) ? "" : reader.GetString(10)
            });
        }

        return items;
    }

    public List<AnalysisResultDto> GetAnalysesByZone(string zona)
    {
        List<AnalysisResultDto> items = new();

        using SqlConnection connection = new(_connectionString);
        connection.Open();

        string sql = @"
SELECT
    Id, MeasurementId, [Timestamp], SensorId, Zona, Tipo, Valor,
    RiskLevel, Summary, RecommendedAction, ProcessedByGateway
FROM AnalysisResults
WHERE UPPER(Zona) = UPPER(@Zona)
ORDER BY [Timestamp] DESC;";

        using SqlCommand cmd = new(sql, connection);
        cmd.Parameters.AddWithValue("@Zona", zona);

        using SqlDataReader reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            items.Add(new AnalysisResultDto
            {
                Id = reader.GetInt32(0),
                MeasurementId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                Timestamp = reader.GetDateTime(2),
                SensorId = reader.GetString(3),
                Zona = reader.GetString(4),
                Tipo = reader.GetString(5),
                Valor = reader.GetDouble(6),
                RiskLevel = reader.GetString(7),
                Summary = reader.GetString(8),
                RecommendedAction = reader.GetString(9),
                ProcessedByGateway = reader.IsDBNull(10) ? "" : reader.GetString(10)
            });
        }

        return items;
    }

    public List<AnalysisResultDto> GetAnalysesByGateway(string gateway)
    {
        List<AnalysisResultDto> items = new();

        using SqlConnection connection = new(_connectionString);
        connection.Open();

        string sql = @"
SELECT
    Id, MeasurementId, [Timestamp], SensorId, Zona, Tipo, Valor,
    RiskLevel, Summary, RecommendedAction, ProcessedByGateway
FROM AnalysisResults
WHERE UPPER(ProcessedByGateway) = UPPER(@Gateway)
ORDER BY [Timestamp] DESC;";

        using SqlCommand cmd = new(sql, connection);
        cmd.Parameters.AddWithValue("@Gateway", gateway);

        using SqlDataReader reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            items.Add(new AnalysisResultDto
            {
                Id = reader.GetInt32(0),
                MeasurementId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                Timestamp = reader.GetDateTime(2),
                SensorId = reader.GetString(3),
                Zona = reader.GetString(4),
                Tipo = reader.GetString(5),
                Valor = reader.GetDouble(6),
                RiskLevel = reader.GetString(7),
                Summary = reader.GetString(8),
                RecommendedAction = reader.GetString(9),
                ProcessedByGateway = reader.IsDBNull(10) ? "" : reader.GetString(10)
            });
        }

        return items;
    }

    public List<MeasurementStatsDto> GetMeasurementStats()
    {
        List<MeasurementStatsDto> items = new();

        using SqlConnection connection = new(_connectionString);
        connection.Open();

        string sql = @"
SELECT
    Tipo,
    COUNT(*) AS TotalRegistos,
    AVG(Valor) AS ValorMedio,
    MIN(Valor) AS ValorMinimo,
    MAX(Valor) AS ValorMaximo
FROM Measurements
GROUP BY Tipo
ORDER BY Tipo;";

        using SqlCommand cmd = new(sql, connection);

        using SqlDataReader reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            items.Add(new MeasurementStatsDto
            {
                Tipo = reader.GetString(0),
                TotalRegistos = reader.GetInt32(1),
                ValorMedio = Convert.ToDouble(reader.GetValue(2)),
                ValorMinimo = Convert.ToDouble(reader.GetValue(3)),
                ValorMaximo = Convert.ToDouble(reader.GetValue(4))
            });
        }

        return items;
    }

    public Dictionary<string, int> GetMeasurementCountByType()
    {
        Dictionary<string, int> data = new(StringComparer.OrdinalIgnoreCase);

        using SqlConnection connection = new(_connectionString);
        connection.Open();

        string sql = @"
SELECT Tipo, COUNT(*)
FROM Measurements
GROUP BY Tipo
ORDER BY Tipo;";

        using SqlCommand cmd = new(sql, connection);
        using SqlDataReader reader = cmd.ExecuteReader();

        while (reader.Read())
        {
            data[reader.GetString(0)] = reader.GetInt32(1);
        }

        return data;
    }

    public Dictionary<string, int> GetAnalysisCountByRisk()
    {
        Dictionary<string, int> data = new(StringComparer.OrdinalIgnoreCase)
        {
            ["LOW"] = 0,
            ["MEDIUM"] = 0,
            ["HIGH"] = 0
        };

        using SqlConnection connection = new(_connectionString);
        connection.Open();

        string sql = @"
SELECT RiskLevel, COUNT(*)
FROM AnalysisResults
GROUP BY RiskLevel;";

        using SqlCommand cmd = new(sql, connection);
        using SqlDataReader reader = cmd.ExecuteReader();

        while (reader.Read())
        {
            string key = reader.GetString(0).ToUpperInvariant();
            data[key] = reader.GetInt32(1);
        }

        return data;
    }
}

public class AnalysisRpcClient
{
    private readonly string _host;
    private readonly int _port;

    public AnalysisRpcClient(string host, int port)
    {
        _host = host;
        _port = port;
    }

    public AnalysisResponse AnalyseManually(AnalysisRequest request)
    {
        try
        {
            using TcpClient rpcClient = new(_host, _port);
            using NetworkStream ns = rpcClient.GetStream();
            using StreamWriter writer = new(ns) { AutoFlush = true };
            using StreamReader reader = new(ns);

            string json = JsonSerializer.Serialize(request);
            writer.WriteLine(json);

            string? responseJson = reader.ReadLine();
            if (responseJson == null)
                return new AnalysisResponse { Ok = false };

            AnalysisResponse? response = JsonSerializer.Deserialize<AnalysisResponse>(responseJson);
            return response ?? new AnalysisResponse { Ok = false };
        }
        catch (Exception ex)
        {
            Console.WriteLine("Erro ao chamar o serviço de análise: " + ex.Message);
            return new AnalysisResponse { Ok = false };
        }
    }
}

public static class AsciiChartRenderer
{
    public static void PrintBarChart(
        string title,
        Dictionary<string, int> data,
        ConsoleColor? defaultColor = null,
        bool riskColors = false)
    {
        Console.WriteLine();
        Console.WriteLine(title);
        Console.WriteLine(new string('=', title.Length));

        if (data.Count == 0)
        {
            Console.WriteLine("Sem dados.");
            return;
        }

        int maxValue = 1;
        foreach (var kvp in data)
        {
            if (kvp.Value > maxValue)
                maxValue = kvp.Value;
        }

        foreach (var kvp in data)
        {
            int barLength = maxValue == 0 ? 0 : (int)Math.Round((kvp.Value / (double)maxValue) * 30);
            string bar = new string('#', barLength);

            Console.Write($"{kvp.Key,-12} | ");

            ConsoleColor original = Console.ForegroundColor;

            if (riskColors)
            {
                switch (kvp.Key.ToUpperInvariant())
                {
                    case "LOW":
                        Console.ForegroundColor = ConsoleColor.Green;
                        break;
                    case "MEDIUM":
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        break;
                    case "HIGH":
                        Console.ForegroundColor = ConsoleColor.Red;
                        break;
                }
            }
            else if (defaultColor.HasValue)
            {
                Console.ForegroundColor = defaultColor.Value;
            }

            Console.Write(bar);
            Console.ForegroundColor = original;
            Console.WriteLine($" ({kvp.Value})");
        }
    }
}

public static class ConsoleRenderer
{
    public static void PrintDashboard(DashboardStatsDto stats)
    {
        Console.WriteLine("========================================================");
        Console.WriteLine("              DASHBOARD INICIAL - MONITORIZAÇÃO         ");
        Console.WriteLine("========================================================");
        Console.WriteLine($"Total de medições : {stats.TotalMeasurements}");
        Console.WriteLine($"Total de vídeos   : {stats.TotalVideos}");
        Console.WriteLine($"Total de análises : {stats.TotalAnalyses}");
        Console.Write("LOW               : ");
        WriteRisk(stats.LowCount.ToString(), "LOW");
        Console.WriteLine();
        Console.Write("MEDIUM            : ");
        WriteRisk(stats.MediumCount.ToString(), "MEDIUM");
        Console.WriteLine();
        Console.Write("HIGH              : ");
        WriteRisk(stats.HighCount.ToString(), "HIGH");
        Console.WriteLine();
        Console.WriteLine("========================================================");
        Console.WriteLine();
    }

    public static void PrintMeasurements(List<MeasurementDto> items)
    {
        if (items.Count == 0)
        {
            Console.WriteLine("Sem medições.");
            return;
        }

        Console.WriteLine();
        Console.WriteLine("ID | Timestamp           | Sensor | Zona          | Tipo   | Valor | Gateway");
        Console.WriteLine("--------------------------------------------------------------------------------");

        foreach (MeasurementDto item in items)
        {
            Console.WriteLine(
                $"{item.Id,-2} | {item.Timestamp:yyyy-MM-dd HH:mm:ss} | {item.SensorId,-6} | {item.Zona,-13} | {item.Tipo,-6} | {item.Valor,-5} | {item.ProcessedByGateway}");
        }
    }

    public static void PrintVideos(List<VideoEventDto> items)
    {
        if (items.Count == 0)
        {
            Console.WriteLine("Sem vídeos.");
            return;
        }

        Console.WriteLine();
        Console.WriteLine("ID | Timestamp           | Sensor | Ficheiro           | Gateway | Caminho");
        Console.WriteLine("--------------------------------------------------------------------------------");

        foreach (VideoEventDto item in items)
        {
            Console.WriteLine(
                $"{item.Id,-2} | {item.Timestamp:yyyy-MM-dd HH:mm:ss} | {item.SensorId,-6} | {item.FileName,-18} | {item.ProcessedByGateway,-7} | {item.FilePath}");
        }
    }

    public static void PrintAnalyses(List<AnalysisResultDto> items)
    {
        if (items.Count == 0)
        {
            Console.WriteLine("Sem análises.");
            return;
        }

        foreach (AnalysisResultDto item in items)
        {
            Console.WriteLine();
            Console.WriteLine($"Análise #{item.Id}");
            Console.WriteLine($"Timestamp: {item.Timestamp:yyyy-MM-dd HH:mm:ss}");
            Console.WriteLine($"Sensor: {item.SensorId}");
            Console.WriteLine($"Zona: {item.Zona}");
            Console.WriteLine($"Tipo: {item.Tipo}");
            Console.WriteLine($"Valor: {item.Valor}");
            Console.WriteLine($"Gateway: {item.ProcessedByGateway}");
            Console.Write("Risco: ");
            WriteRisk(item.RiskLevel, item.RiskLevel);
            Console.WriteLine();
            Console.WriteLine($"Resumo: {item.Summary}");
            Console.WriteLine($"Ação: {item.RecommendedAction}");
            Console.WriteLine(new string('-', 60));
        }
    }

    public static void PrintManualAnalysis(AnalysisRequest request, AnalysisResponse response)
    {
        Console.WriteLine();
        Console.WriteLine("Resultado da análise manual");
        Console.WriteLine("---------------------------");
        Console.WriteLine($"Sensor: {request.SensorId}");
        Console.WriteLine($"Zona: {request.Zona}");
        Console.WriteLine($"Tipo: {request.Tipo}");
        Console.WriteLine($"Valor: {request.Valor}");
        Console.Write("Risco: ");
        WriteRisk(response.RiskLevel, response.RiskLevel);
        Console.WriteLine();
        Console.WriteLine($"Resumo: {response.Summary}");
        Console.WriteLine($"Ação: {response.RecommendedAction}");
    }

    public static void PrintStats(List<MeasurementStatsDto> items)
    {
        if (items.Count == 0)
        {
            Console.WriteLine("Sem estatísticas.");
            return;
        }

        Console.WriteLine();
        Console.WriteLine("Tipo   | Total | Média   | Mínimo  | Máximo");
        Console.WriteLine("---------------------------------------------");

        foreach (MeasurementStatsDto item in items)
        {
            Console.WriteLine(
                $"{item.Tipo,-6} | {item.TotalRegistos,-5} | {item.ValorMedio,-7:F2} | {item.ValorMinimo,-7:F2} | {item.ValorMaximo,-7:F2}");
        }
    }

    public static void WriteRisk(string text, string riskLevel)
    {
        ConsoleColor original = Console.ForegroundColor;

        switch (riskLevel.ToUpperInvariant())
        {
            case "LOW":
                Console.ForegroundColor = ConsoleColor.Green;
                break;
            case "MEDIUM":
                Console.ForegroundColor = ConsoleColor.Yellow;
                break;
            case "HIGH":
                Console.ForegroundColor = ConsoleColor.Red;
                break;
        }

        Console.Write(text);
        Console.ForegroundColor = original;
    }
}

public class CliApplication
{
    private readonly DatabaseRepository _repository;
    private readonly AnalysisRpcClient _analysisRpcClient;

    public CliApplication(DatabaseRepository repository, AnalysisRpcClient analysisRpcClient)
    {
        _repository = repository;
        _analysisRpcClient = analysisRpcClient;
    }

    public void Run()
    {
        Console.Clear();
        ShowDashboard();

        while (true)
        {
            ShowMenu();
            Console.Write("Escolha: ");
            string? option = Console.ReadLine()?.Trim();

            Console.WriteLine();

            switch (option)
            {
                case "1":
                    ShowDashboard();
                    break;
                case "2":
                    ShowLatestMeasurements();
                    break;
                case "3":
                    ShowMeasurementsBySensor();
                    break;
                case "4":
                    ShowMeasurementsByZone();
                    break;
                case "5":
                    ShowMeasurementsByGateway();
                    break;
                case "6":
                    ShowLatestVideos();
                    break;
                case "7":
                    OpenVideoPath();
                    break;
                case "8":
                    ShowLatestAnalyses();
                    break;
                case "9":
                    ShowAnalysesByRisk();
                    break;
                case "10":
                    ShowAnalysesByZone();
                    break;
                case "11":
                    ShowAnalysesByGateway();
                    break;
                case "12":
                    ShowStats();
                    break;
                case "13":
                    ShowAsciiCharts();
                    break;
                case "14":
                    RequestManualAnalysis();
                    break;
                case "0":
                    Console.WriteLine("A terminar...");
                    return;
                default:
                    Console.WriteLine("Opção inválida.");
                    break;
            }

            Console.WriteLine();
            Console.WriteLine("Prima ENTER para continuar...");
            Console.ReadLine();
            Console.Clear();
        }
    }

    private static void ShowMenu()
    {
        Console.WriteLine("========================================================");
        Console.WriteLine("      INTERFACE CLI - MONITORIZAÇÃO MULTI-GATEWAY       ");
        Console.WriteLine("========================================================");
        Console.WriteLine("1  - Ver dashboard inicial");
        Console.WriteLine("2  - Ver últimas medições");
        Console.WriteLine("3  - Ver medições por sensor");
        Console.WriteLine("4  - Pesquisar medições por zona");
        Console.WriteLine("5  - Ver medições por gateway");
        Console.WriteLine("6  - Ver últimos vídeos");
        Console.WriteLine("7  - Abrir caminho de um vídeo guardado");
        Console.WriteLine("8  - Ver últimas análises");
        Console.WriteLine("9  - Ver análises por nível de risco");
        Console.WriteLine("10 - Pesquisar análises por zona");
        Console.WriteLine("11 - Ver análises por gateway");
        Console.WriteLine("12 - Ver estatísticas rápidas");
        Console.WriteLine("13 - Ver gráficos ASCII");
        Console.WriteLine("14 - Fazer pedido de análise manual");
        Console.WriteLine("0  - Sair");
        Console.WriteLine("========================================================");
    }

    private void ShowDashboard()
    {
        DashboardStatsDto stats = _repository.GetDashboardStats();
        ConsoleRenderer.PrintDashboard(stats);
    }

    private void ShowLatestMeasurements()
    {
        Console.Write("Quantas medições quer ver? ");
        int top = ReadPositiveIntOrDefault(10);

        List<MeasurementDto> items = _repository.GetLatestMeasurements(top);
        ConsoleRenderer.PrintMeasurements(items);
    }

    private void ShowMeasurementsBySensor()
    {
        Console.Write("SensorId: ");
        string sensorId = Console.ReadLine()?.Trim() ?? "";

        Console.Write("Quantas medições quer ver? ");
        int top = ReadPositiveIntOrDefault(10);

        List<MeasurementDto> items = _repository.GetMeasurementsBySensor(sensorId, top);
        ConsoleRenderer.PrintMeasurements(items);
    }

    private void ShowMeasurementsByZone()
    {
        Console.Write("Zona: ");
        string zona = Console.ReadLine()?.Trim() ?? "";

        Console.Write("Quantas medições quer ver? ");
        int top = ReadPositiveIntOrDefault(10);

        List<MeasurementDto> items = _repository.GetMeasurementsByZone(zona, top);
        ConsoleRenderer.PrintMeasurements(items);
    }

    private void ShowMeasurementsByGateway()
    {
        Console.Write("Gateway (ex: G1, G2): ");
        string gateway = Console.ReadLine()?.Trim() ?? "";

        Console.Write("Quantas medições quer ver? ");
        int top = ReadPositiveIntOrDefault(10);

        List<MeasurementDto> items = _repository.GetMeasurementsByGateway(gateway, top);
        ConsoleRenderer.PrintMeasurements(items);
    }

    private void ShowLatestVideos()
    {
        Console.Write("Quantos vídeos quer ver? ");
        int top = ReadPositiveIntOrDefault(10);

        List<VideoEventDto> items = _repository.GetLatestVideos(top);
        ConsoleRenderer.PrintVideos(items);
    }

    private void OpenVideoPath()
    {
        Console.Write("ID do vídeo a abrir: ");
        int id = ReadPositiveIntOrDefault(-1);

        if (id <= 0)
        {
            Console.WriteLine("ID inválido.");
            return;
        }

        VideoEventDto? video = _repository.GetVideoById(id);
        if (video == null)
        {
            Console.WriteLine("Vídeo não encontrado.");
            return;
        }

        if (!File.Exists(video.FilePath))
        {
            Console.WriteLine("O ficheiro não existe no caminho guardado.");
            Console.WriteLine(video.FilePath);
            return;
        }

        try
        {
            Process.Start(new ProcessStartInfo
            {
                FileName = video.FilePath,
                UseShellExecute = true
            });

            Console.WriteLine("Vídeo aberto com sucesso.");
        }
        catch (Exception ex)
        {
            Console.WriteLine("Erro ao abrir o vídeo: " + ex.Message);
        }
    }

    private void ShowLatestAnalyses()
    {
        Console.Write("Quantas análises quer ver? ");
        int top = ReadPositiveIntOrDefault(10);

        List<AnalysisResultDto> items = _repository.GetLatestAnalyses(top);
        ConsoleRenderer.PrintAnalyses(items);
    }

    private void ShowAnalysesByRisk()
    {
        Console.Write("Nível de risco (LOW, MEDIUM, HIGH): ");
        string risk = (Console.ReadLine()?.Trim() ?? "").ToUpperInvariant();

        List<AnalysisResultDto> items = _repository.GetAnalysesByRisk(risk);
        ConsoleRenderer.PrintAnalyses(items);
    }

    private void ShowAnalysesByZone()
    {
        Console.Write("Zona: ");
        string zona = Console.ReadLine()?.Trim() ?? "";

        List<AnalysisResultDto> items = _repository.GetAnalysesByZone(zona);
        ConsoleRenderer.PrintAnalyses(items);
    }

    private void ShowAnalysesByGateway()
    {
        Console.Write("Gateway (ex: G1, G2): ");
        string gateway = Console.ReadLine()?.Trim() ?? "";

        List<AnalysisResultDto> items = _repository.GetAnalysesByGateway(gateway);
        ConsoleRenderer.PrintAnalyses(items);
    }

    private void ShowStats()
    {
        List<MeasurementStatsDto> items = _repository.GetMeasurementStats();
        ConsoleRenderer.PrintStats(items);
    }

    private void ShowAsciiCharts()
    {
        Dictionary<string, int> measurementCounts = _repository.GetMeasurementCountByType();
        Dictionary<string, int> riskCounts = _repository.GetAnalysisCountByRisk();

        AsciiChartRenderer.PrintBarChart("Gráfico ASCII - Registos por Tipo", measurementCounts, ConsoleColor.Cyan, false);
        AsciiChartRenderer.PrintBarChart("Gráfico ASCII - Análises por Risco", riskCounts, null, true);
    }

    private void RequestManualAnalysis()
    {
        Console.Write("SensorId: ");
        string sensorId = Console.ReadLine()?.Trim() ?? "";

        Console.Write("Zona: ");
        string zona = Console.ReadLine()?.Trim() ?? "";

        Console.Write("Tipo (TEMP, AR, RUIDO, HUM): ");
        string tipo = (Console.ReadLine()?.Trim() ?? "").ToUpperInvariant();

        Console.Write("Valor: ");
        string? valorInput = Console.ReadLine()?.Trim();

        if (!double.TryParse(
                valorInput?.Replace(',', '.'),
                System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture,
                out double valor))
        {
            Console.WriteLine("Valor inválido.");
            return;
        }

        AnalysisRequest request = new()
        {
            Timestamp = DateTime.Now.ToString("s"),
            SensorId = sensorId,
            Zona = zona,
            Tipo = tipo,
            Valor = valor
        };

        AnalysisResponse response = _analysisRpcClient.AnalyseManually(request);

        if (!response.Ok)
        {
            Console.WriteLine("Falha no pedido de análise.");
            return;
        }

        ConsoleRenderer.PrintManualAnalysis(request, response);
    }

    private static int ReadPositiveIntOrDefault(int fallback)
    {
        string? input = Console.ReadLine()?.Trim();

        if (!int.TryParse(input, out int value) || value <= 0)
            return fallback;

        return value;
    }
}

public static class Program
{
    public static void Main(string[] args)
    {
        string connectionString =
            @"Server=(localdb)\MSSQLLocalDB;Database=MonitorizacaoUrbana;Trusted_Connection=True;TrustServerCertificate=True;";

        DatabaseRepository repository = new(connectionString);
        AnalysisRpcClient analysisRpcClient = new("127.0.0.1", 8000);
        CliApplication app = new(repository, analysisRpcClient);
        app.Run();
    }
}