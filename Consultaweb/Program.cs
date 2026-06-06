using System.Diagnostics;
using System.Globalization;
using System.Net.Sockets;
using System.Text.Json;
using Microsoft.Data.SqlClient;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton(new DatabaseRepository(
    @"Server=(localdb)\MSSQLLocalDB;Database=MonitorizacaoUrbana;Trusted_Connection=True;TrustServerCertificate=True;"));

builder.Services.AddSingleton(new AnalysisRpcClient("127.0.0.1", 8000));

var app = builder.Build();

app.MapGet("/", () => Results.Content(HtmlPages.Index, "text/html; charset=utf-8"));

app.MapGet("/api/dashboard", (DatabaseRepository repo) =>
{
    return Results.Json(repo.GetDashboardStats());
});

app.MapGet("/api/measurements/latest", (DatabaseRepository repo, int? top) =>
{
    return Results.Json(repo.GetLatestMeasurements(top ?? 10));
});

app.MapGet("/api/measurements/sensor/{sensorId}", (DatabaseRepository repo, string sensorId, int? top) =>
{
    return Results.Json(repo.GetMeasurementsBySensor(sensorId, top ?? 10));
});

app.MapGet("/api/measurements/zone/{zona}", (DatabaseRepository repo, string zona, int? top) =>
{
    return Results.Json(repo.GetMeasurementsByZone(zona, top ?? 10));
});

app.MapGet("/api/measurements/gateway/{gateway}", (DatabaseRepository repo, string gateway, int? top) =>
{
    return Results.Json(repo.GetMeasurementsByGateway(gateway, top ?? 10));
});

app.MapGet("/api/videos/latest", (DatabaseRepository repo, int? top) =>
{
    return Results.Json(repo.GetLatestVideos(top ?? 10));
});

app.MapGet("/api/videos/{id:int}", (DatabaseRepository repo, int id) =>
{
    var video = repo.GetVideoById(id);
    return video is null ? Results.NotFound() : Results.Json(video);
});

app.MapGet("/api/analyses/latest", (DatabaseRepository repo, int? top) =>
{
    return Results.Json(repo.GetLatestAnalyses(top ?? 10));
});

app.MapGet("/api/analyses/risk/{risk}", (DatabaseRepository repo, string risk) =>
{
    return Results.Json(repo.GetAnalysesByRisk(risk));
});

app.MapGet("/api/analyses/zone/{zona}", (DatabaseRepository repo, string zona) =>
{
    return Results.Json(repo.GetAnalysesByZone(zona));
});

app.MapGet("/api/analyses/gateway/{gateway}", (DatabaseRepository repo, string gateway) =>
{
    return Results.Json(repo.GetAnalysesByGateway(gateway));
});

app.MapGet("/api/stats", (DatabaseRepository repo) =>
{
    return Results.Json(repo.GetMeasurementStats());
});

app.MapGet("/api/charts/measurements-by-type", (DatabaseRepository repo) =>
{
    return Results.Json(repo.GetMeasurementCountByType());
});

app.MapGet("/api/charts/analyses-by-risk", (DatabaseRepository repo) =>
{
    return Results.Json(repo.GetAnalysisCountByRisk());
});

app.MapPost("/api/manual-analysis", async (HttpContext context, AnalysisRpcClient rpcClient) =>
{
    var request = await context.Request.ReadFromJsonAsync<AnalysisRequest>();
    if (request == null)
        return Results.BadRequest(new { error = "Pedido inválido." });

    var response = rpcClient.AnalyseManually(request);
    return Results.Json(response);
});

app.Run();

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
            stats.TotalMeasurements = Convert.ToInt32(cmd.ExecuteScalar());

        using (SqlCommand cmd = new("SELECT COUNT(*) FROM VideoEvents;", connection))
            stats.TotalVideos = Convert.ToInt32(cmd.ExecuteScalar());

        using (SqlCommand cmd = new("SELECT COUNT(*) FROM AnalysisResults;", connection))
            stats.TotalAnalyses = Convert.ToInt32(cmd.ExecuteScalar());

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
        if (!reader.Read()) return null;

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

public static class HtmlPages
{
    public static string Index => """
<!DOCTYPE html>
<html lang="pt">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Monitorização Urbana</title>
  <style>
    body{font-family:Arial,sans-serif;background:#0f172a;color:#e2e8f0;margin:0;padding:24px}
    h1,h2{margin:0 0 12px}
    .wrap{max-width:1200px;margin:0 auto}
    .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:16px;margin-bottom:24px}
    .card{background:#1e293b;border-radius:16px;padding:16px;box-shadow:0 4px 20px rgba(0,0,0,.25)}
    .tabs{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:20px}
    .tabs button{background:#334155;color:#fff;border:0;padding:10px 14px;border-radius:10px;cursor:pointer}
    .tabs button.active{background:#2563eb}
    .panel{display:none}
    .panel.active{display:block}
    table{width:100%;border-collapse:collapse;background:#1e293b;border-radius:12px;overflow:hidden}
    th,td{padding:10px;border-bottom:1px solid #334155;text-align:left;font-size:14px}
    th{background:#334155}
    input,select{padding:10px;border-radius:10px;border:1px solid #475569;background:#0f172a;color:#fff}
    .row{display:flex;gap:10px;flex-wrap:wrap;margin:10px 0 16px}
    .risk-low{color:#22c55e;font-weight:bold}
    .risk-medium{color:#eab308;font-weight:bold}
    .risk-high{color:#ef4444;font-weight:bold}
    .chart-bar{height:26px;background:#334155;border-radius:8px;overflow:hidden;margin:8px 0}
    .chart-fill{height:100%;background:#38bdf8}
    .chart-fill.low{background:#22c55e}
    .chart-fill.medium{background:#eab308}
    .chart-fill.high{background:#ef4444}
    .muted{color:#94a3b8}
    .analysis-box{background:#1e293b;padding:16px;border-radius:12px;margin-top:12px}
    a{color:#60a5fa}
  </style>
</head>
<body>
<div class="wrap">
  <h1>Monitorização Urbana</h1>
  <p class="muted">Dashboard HTML da antiga ConsultaCLI.</p>

  <div id="dashboard" class="grid"></div>

  <div class="tabs">
    <button class="tab-btn active" data-tab="medicoes">Medições</button>
    <button class="tab-btn" data-tab="videos">Vídeos</button>
    <button class="tab-btn" data-tab="analises">Análises</button>
    <button class="tab-btn" data-tab="stats">Estatísticas</button>
    <button class="tab-btn" data-tab="manual">Análise Manual</button>
  </div>

  <section id="medicoes" class="panel active">
    <h2>Medições</h2>
    <div class="row">
      <button onclick="loadLatestMeasurements()">Últimas</button>
      <input id="sensorIdInput" placeholder="SensorId" />
      <button onclick="loadMeasurementsBySensor()">Por sensor</button>
      <input id="zonaInput" placeholder="Zona" />
      <button onclick="loadMeasurementsByZone()">Por zona</button>
      <input id="gatewayInput" placeholder="Gateway" />
      <button onclick="loadMeasurementsByGateway()">Por gateway</button>
    </div>
    <div id="measurementsTable"></div>
  </section>

  <section id="videos" class="panel">
    <h2>Vídeos</h2>
    <div class="row">
      <button onclick="loadVideos()">Carregar vídeos</button>
    </div>
    <div id="videosTable"></div>
  </section>

  <section id="analises" class="panel">
    <h2>Análises</h2>
    <div class="row">
      <button onclick="loadLatestAnalyses()">Últimas</button>
      <input id="riskInput" placeholder="LOW / MEDIUM / HIGH" />
      <button onclick="loadAnalysesByRisk()">Por risco</button>
      <input id="analysisZonaInput" placeholder="Zona" />
      <button onclick="loadAnalysesByZone()">Por zona</button>
      <input id="analysisGatewayInput" placeholder="Gateway" />
      <button onclick="loadAnalysesByGateway()">Por gateway</button>
    </div>
    <div id="analysesTable"></div>
  </section>

  <section id="stats" class="panel">
    <h2>Estatísticas</h2>
    <div class="row">
      <button onclick="loadStats()">Tabela de estatísticas</button>
      <button onclick="loadCharts()">Gráficos</button>
    </div>
    <div id="statsTable"></div>
    <div id="chartsArea"></div>
  </section>

  <section id="manual" class="panel">
    <h2>Pedido de análise manual</h2>
    <div class="row">
      <input id="manualSensor" placeholder="SensorId" />
      <input id="manualZona" placeholder="Zona" />
      <select id="manualTipo">
        <option value="TEMP">TEMP</option>
        <option value="AR">AR</option>
        <option value="RUIDO">RUIDO</option>
        <option value="HUM">HUM</option>
      </select>
      <input id="manualValor" placeholder="Valor" />
      <button onclick="submitManualAnalysis()">Analisar</button>
    </div>
    <div id="manualResult"></div>
  </section>
</div>

<script>
const $ = id => document.getElementById(id);

document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    $(btn.dataset.tab).classList.add('active');
  });
});

function riskClass(risk){
  risk = (risk || '').toUpperCase();
  if(risk === 'LOW') return 'risk-low';
  if(risk === 'MEDIUM') return 'risk-medium';
  if(risk === 'HIGH') return 'risk-high';
  return '';
}

async function loadDashboard(){
  const data = await fetch('/api/dashboard').then(r => r.json());
  $('dashboard').innerHTML = `
    <div class="card"><h3>Medições</h3><div>${data.totalMeasurements}</div></div>
    <div class="card"><h3>Vídeos</h3><div>${data.totalVideos}</div></div>
    <div class="card"><h3>Análises</h3><div>${data.totalAnalyses}</div></div>
    <div class="card"><h3>LOW</h3><div class="risk-low">${data.lowCount}</div></div>
    <div class="card"><h3>MEDIUM</h3><div class="risk-medium">${data.mediumCount}</div></div>
    <div class="card"><h3>HIGH</h3><div class="risk-high">${data.highCount}</div></div>
  `;
}

function renderTable(headers, rows){
  let html = '<table><thead><tr>' + headers.map(h => `<th>${h}</th>`).join('') + '</tr></thead><tbody>';
  html += rows.map(row => '<tr>' + row.map(cell => `<td>${cell}</td>`).join('') + '</tr>').join('');
  html += '</tbody></table>';
  return html;
}

async function loadLatestMeasurements(){
  const data = await fetch('/api/measurements/latest?top=20').then(r => r.json());
  $('measurementsTable').innerHTML = renderTable(
    ['ID','Timestamp','Sensor','Zona','Tipo','Valor','Gateway'],
    data.map(x => [x.id, x.timestamp, x.sensorId, x.zona, x.tipo, x.valor, x.processedByGateway])
  );
}

async function loadMeasurementsBySensor(){
  const sensorId = $('sensorIdInput').value.trim();
  if(!sensorId) return;
  const data = await fetch('/api/measurements/sensor/' + encodeURIComponent(sensorId) + '?top=20').then(r => r.json());
  $('measurementsTable').innerHTML = renderTable(
    ['ID','Timestamp','Sensor','Zona','Tipo','Valor','Gateway'],
    data.map(x => [x.id, x.timestamp, x.sensorId, x.zona, x.tipo, x.valor, x.processedByGateway])
  );
}

async function loadMeasurementsByZone(){
  const zona = $('zonaInput').value.trim();
  if(!zona) return;
  const data = await fetch('/api/measurements/zone/' + encodeURIComponent(zona) + '?top=20').then(r => r.json());
  $('measurementsTable').innerHTML = renderTable(
    ['ID','Timestamp','Sensor','Zona','Tipo','Valor','Gateway'],
    data.map(x => [x.id, x.timestamp, x.sensorId, x.zona, x.tipo, x.valor, x.processedByGateway])
  );
}

async function loadMeasurementsByGateway(){
  const gateway = $('gatewayInput').value.trim();
  if(!gateway) return;
  const data = await fetch('/api/measurements/gateway/' + encodeURIComponent(gateway) + '?top=20').then(r => r.json());
  $('measurementsTable').innerHTML = renderTable(
    ['ID','Timestamp','Sensor','Zona','Tipo','Valor','Gateway'],
    data.map(x => [x.id, x.timestamp, x.sensorId, x.zona, x.tipo, x.valor, x.processedByGateway])
  );
}

async function loadVideos(){
  const data = await fetch('/api/videos/latest?top=20').then(r => r.json());
  $('videosTable').innerHTML = renderTable(
    ['ID','Timestamp','Sensor','Zona','Ficheiro','Tamanho','Gateway','Caminho'],
    data.map(x => [x.id, x.timestamp, x.sensorId, x.zona, x.fileName, x.fileSize, x.processedByGateway, x.filePath])
  );
}

async function loadLatestAnalyses(){
  const data = await fetch('/api/analyses/latest?top=20').then(r => r.json());
  $('analysesTable').innerHTML = renderTable(
    ['ID','Timestamp','Sensor','Zona','Tipo','Valor','Risco','Resumo','Ação','Gateway'],
    data.map(x => [x.id, x.timestamp, x.sensorId, x.zona, x.tipo, x.valor, `<span class="${riskClass(x.riskLevel)}">${x.riskLevel}</span>`, x.summary, x.recommendedAction, x.processedByGateway])
  );
}

async function loadAnalysesByRisk(){
  const risk = $('riskInput').value.trim();
  if(!risk) return;
  const data = await fetch('/api/analyses/risk/' + encodeURIComponent(risk)).then(r => r.json());
  $('analysesTable').innerHTML = renderTable(
    ['ID','Timestamp','Sensor','Zona','Tipo','Valor','Risco','Resumo','Ação','Gateway'],
    data.map(x => [x.id, x.timestamp, x.sensorId, x.zona, x.tipo, x.valor, `<span class="${riskClass(x.riskLevel)}">${x.riskLevel}</span>`, x.summary, x.recommendedAction, x.processedByGateway])
  );
}

async function loadAnalysesByZone(){
  const zona = $('analysisZonaInput').value.trim();
  if(!zona) return;
  const data = await fetch('/api/analyses/zone/' + encodeURIComponent(zona)).then(r => r.json());
  $('analysesTable').innerHTML = renderTable(
    ['ID','Timestamp','Sensor','Zona','Tipo','Valor','Risco','Resumo','Ação','Gateway'],
    data.map(x => [x.id, x.timestamp, x.sensorId, x.zona, x.tipo, x.valor, `<span class="${riskClass(x.riskLevel)}">${x.riskLevel}</span>`, x.summary, x.recommendedAction, x.processedByGateway])
  );
}

async function loadAnalysesByGateway(){
  const gateway = $('analysisGatewayInput').value.trim();
  if(!gateway) return;
  const data = await fetch('/api/analyses/gateway/' + encodeURIComponent(gateway)).then(r => r.json());
  $('analysesTable').innerHTML = renderTable(
    ['ID','Timestamp','Sensor','Zona','Tipo','Valor','Risco','Resumo','Ação','Gateway'],
    data.map(x => [x.id, x.timestamp, x.sensorId, x.zona, x.tipo, x.valor, `<span class="${riskClass(x.riskLevel)}">${x.riskLevel}</span>`, x.summary, x.recommendedAction, x.processedByGateway])
  );
}

async function loadStats(){
  const data = await fetch('/api/stats').then(r => r.json());
  $('statsTable').innerHTML = renderTable(
    ['Tipo','Total','Média','Mínimo','Máximo'],
    data.map(x => [x.tipo, x.totalRegistos, x.valorMedio.toFixed(2), x.valorMinimo.toFixed(2), x.valorMaximo.toFixed(2)])
  );
}

async function loadCharts(){
  const byType = await fetch('/api/charts/measurements-by-type').then(r => r.json());
  const byRisk = await fetch('/api/charts/analyses-by-risk').then(r => r.json());

  let html = '<div class="card"><h3>Registos por Tipo</h3>';
  const maxType = Math.max(...Object.values(byType), 1);
  for (const [k,v] of Object.entries(byType)) {
    const width = (v / maxType) * 100;
    html += `<div>${k} (${v})</div><div class="chart-bar"><div class="chart-fill" style="width:${width}%"></div></div>`;
  }
  html += '</div><div class="card"><h3>Análises por Risco</h3>';
  const maxRisk = Math.max(...Object.values(byRisk), 1);
  for (const [k,v] of Object.entries(byRisk)) {
    const width = (v / maxRisk) * 100;
    const klass = k.toUpperCase() === 'LOW' ? 'low' : k.toUpperCase() === 'MEDIUM' ? 'medium' : 'high';
    html += `<div>${k} (${v})</div><div class="chart-bar"><div class="chart-fill ${klass}" style="width:${width}%"></div></div>`;
  }
  html += '</div>';

  $('chartsArea').innerHTML = html;
}

async function submitManualAnalysis(){
  const body = {
    timestamp: new Date().toISOString(),
    sensorId: $('manualSensor').value.trim(),
    zona: $('manualZona').value.trim(),
    tipo: $('manualTipo').value,
    valor: Number(($('manualValor').value || '0').replace(',', '.'))
  };

  const data = await fetch('/api/manual-analysis', {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify(body)
  }).then(r => r.json());

  $('manualResult').innerHTML = `
    <div class="analysis-box">
      <p><strong>Risco:</strong> <span class="${riskClass(data.riskLevel)}">${data.riskLevel}</span></p>
      <p><strong>Resumo:</strong> ${data.summary}</p>
      <p><strong>Ação:</strong> ${data.recommendedAction}</p>
    </div>
  `;
}

loadDashboard();
loadLatestMeasurements();
</script>
</body>
</html>
""";
}