using System;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using Microsoft.Data.SqlClient;

namespace ServidorApp;

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

public class MeasurementRecord
{
    public DateTime Timestamp { get; set; }
    public string SensorId { get; set; } = "";
    public string Zona { get; set; } = "";
    public string Tipo { get; set; } = "";
    public double Valor { get; set; }
    public string ProcessedByGateway { get; set; } = "";
}

public class VideoRecord
{
    public DateTime Timestamp { get; set; }
    public string SensorId { get; set; } = "";
    public string Zona { get; set; } = "";
    public string FileName { get; set; } = "";
    public string FilePath { get; set; } = "";
    public long FileSize { get; set; }
    public string ProcessedByGateway { get; set; } = "";
}

public class SqlServerRepository
{
    private readonly string _connectionString;
    private readonly object _dbLock = new();

    public SqlServerRepository(string connectionString)
    {
        _connectionString = connectionString;
    }

    public void EnsureTables()
    {
        lock (_dbLock)
        {
            using SqlConnection connection = new(_connectionString);
            connection.Open();

            string sql = @"
IF OBJECT_ID('dbo.Measurements', 'U') IS NULL
BEGIN
    CREATE TABLE Measurements (
        Id INT IDENTITY(1,1) PRIMARY KEY,
        [Timestamp] DATETIME2 NOT NULL,
        SensorId NVARCHAR(50) NOT NULL,
        Zona NVARCHAR(100) NOT NULL,
        Tipo NVARCHAR(50) NOT NULL,
        Valor FLOAT NOT NULL
    );
END

IF OBJECT_ID('dbo.VideoEvents', 'U') IS NULL
BEGIN
    CREATE TABLE VideoEvents (
        Id INT IDENTITY(1,1) PRIMARY KEY,
        [Timestamp] DATETIME2 NOT NULL,
        SensorId NVARCHAR(50) NOT NULL,
        Zona NVARCHAR(100) NOT NULL,
        FileName NVARCHAR(260) NOT NULL,
        FilePath NVARCHAR(500) NOT NULL,
        FileSize BIGINT NOT NULL
    );
END

IF OBJECT_ID('dbo.AnalysisResults', 'U') IS NULL
BEGIN
    CREATE TABLE AnalysisResults (
        Id INT IDENTITY(1,1) PRIMARY KEY,
        MeasurementId INT NULL,
        [Timestamp] DATETIME2 NOT NULL,
        SensorId NVARCHAR(50) NOT NULL,
        Zona NVARCHAR(100) NOT NULL,
        Tipo NVARCHAR(50) NOT NULL,
        Valor FLOAT NOT NULL,
        RiskLevel NVARCHAR(50) NOT NULL,
        Summary NVARCHAR(500) NOT NULL,
        RecommendedAction NVARCHAR(500) NOT NULL
    );
END

IF COL_LENGTH('dbo.Measurements', 'ProcessedByGateway') IS NULL
BEGIN
    ALTER TABLE Measurements
    ADD ProcessedByGateway NVARCHAR(100) NOT NULL CONSTRAINT DF_Measurements_ProcessedByGateway DEFAULT '';
END

IF COL_LENGTH('dbo.VideoEvents', 'ProcessedByGateway') IS NULL
BEGIN
    ALTER TABLE VideoEvents
    ADD ProcessedByGateway NVARCHAR(100) NOT NULL CONSTRAINT DF_VideoEvents_ProcessedByGateway DEFAULT '';
END

IF COL_LENGTH('dbo.AnalysisResults', 'ProcessedByGateway') IS NULL
BEGIN
    ALTER TABLE AnalysisResults
    ADD ProcessedByGateway NVARCHAR(100) NOT NULL CONSTRAINT DF_AnalysisResults_ProcessedByGateway DEFAULT '';
END";

            using SqlCommand cmd = new(sql, connection);
            cmd.ExecuteNonQuery();
        }
    }

    public int InsertMeasurement(MeasurementRecord measurement)
    {
        lock (_dbLock)
        {
            using SqlConnection connection = new(_connectionString);
            connection.Open();

            string sql = @"
INSERT INTO Measurements ([Timestamp], SensorId, Zona, Tipo, Valor, ProcessedByGateway)
OUTPUT INSERTED.Id
VALUES (@Timestamp, @SensorId, @Zona, @Tipo, @Valor, @ProcessedByGateway);";

            using SqlCommand cmd = new(sql, connection);
            cmd.Parameters.AddWithValue("@Timestamp", measurement.Timestamp);
            cmd.Parameters.AddWithValue("@SensorId", measurement.SensorId);
            cmd.Parameters.AddWithValue("@Zona", measurement.Zona);
            cmd.Parameters.AddWithValue("@Tipo", measurement.Tipo);
            cmd.Parameters.AddWithValue("@Valor", measurement.Valor);
            cmd.Parameters.AddWithValue("@ProcessedByGateway", measurement.ProcessedByGateway);

            object? result = cmd.ExecuteScalar();
            return Convert.ToInt32(result);
        }
    }

    public void InsertVideoEvent(VideoRecord video)
    {
        lock (_dbLock)
        {
            using SqlConnection connection = new(_connectionString);
            connection.Open();

            string sql = @"
INSERT INTO VideoEvents ([Timestamp], SensorId, Zona, FileName, FilePath, FileSize, ProcessedByGateway)
VALUES (@Timestamp, @SensorId, @Zona, @FileName, @FilePath, @FileSize, @ProcessedByGateway);";

            using SqlCommand cmd = new(sql, connection);
            cmd.Parameters.AddWithValue("@Timestamp", video.Timestamp);
            cmd.Parameters.AddWithValue("@SensorId", video.SensorId);
            cmd.Parameters.AddWithValue("@Zona", video.Zona);
            cmd.Parameters.AddWithValue("@FileName", video.FileName);
            cmd.Parameters.AddWithValue("@FilePath", video.FilePath);
            cmd.Parameters.AddWithValue("@FileSize", video.FileSize);
            cmd.Parameters.AddWithValue("@ProcessedByGateway", video.ProcessedByGateway);

            cmd.ExecuteNonQuery();
        }
    }

    public void InsertAnalysisResult(int measurementId, MeasurementRecord measurement, AnalysisResponse analysis)
    {
        lock (_dbLock)
        {
            using SqlConnection connection = new(_connectionString);
            connection.Open();

            string sql = @"
INSERT INTO AnalysisResults
(MeasurementId, [Timestamp], SensorId, Zona, Tipo, Valor, RiskLevel, Summary, RecommendedAction, ProcessedByGateway)
VALUES
(@MeasurementId, @Timestamp, @SensorId, @Zona, @Tipo, @Valor, @RiskLevel, @Summary, @RecommendedAction, @ProcessedByGateway);";

            using SqlCommand cmd = new(sql, connection);
            cmd.Parameters.AddWithValue("@MeasurementId", measurementId);
            cmd.Parameters.AddWithValue("@Timestamp", measurement.Timestamp);
            cmd.Parameters.AddWithValue("@SensorId", measurement.SensorId);
            cmd.Parameters.AddWithValue("@Zona", measurement.Zona);
            cmd.Parameters.AddWithValue("@Tipo", measurement.Tipo);
            cmd.Parameters.AddWithValue("@Valor", measurement.Valor);
            cmd.Parameters.AddWithValue("@RiskLevel", analysis.RiskLevel);
            cmd.Parameters.AddWithValue("@Summary", analysis.Summary);
            cmd.Parameters.AddWithValue("@RecommendedAction", analysis.RecommendedAction);
            cmd.Parameters.AddWithValue("@ProcessedByGateway", measurement.ProcessedByGateway);

            cmd.ExecuteNonQuery();
        }
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

    public AnalysisResponse Analyse(MeasurementRecord measurement)
    {
        try
        {
            using TcpClient rpcClient = new(_host, _port);
            using NetworkStream ns = rpcClient.GetStream();
            using StreamWriter writer = new(ns) { AutoFlush = true };
            using StreamReader reader = new(ns);

            var request = new AnalysisRequest
            {
                Timestamp = measurement.Timestamp.ToString("s"),
                SensorId = measurement.SensorId,
                Zona = measurement.Zona,
                Tipo = measurement.Tipo,
                Valor = measurement.Valor
            };

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
            Console.WriteLine("[SERVIDOR] Erro RPC análise: " + ex.Message);
            return new AnalysisResponse { Ok = false };
        }
    }
}

public class FileReceiver
{
    public bool Receive(NetworkStream ns, string fullPath, long totalBytes)
    {
        try
        {
            byte[] buffer = new byte[8192];
            long totalRead = 0;

            using FileStream fs = new(fullPath, FileMode.Create, FileAccess.Write);

            while (totalRead < totalBytes)
            {
                int toRead = (int)Math.Min(buffer.Length, totalBytes - totalRead);
                int bytesRead = ns.Read(buffer, 0, toRead);

                if (bytesRead <= 0)
                    return false;

                fs.Write(buffer, 0, bytesRead);
                totalRead += bytesRead;
            }

            fs.Flush();
            return true;
        }
        catch
        {
            return false;
        }
    }
}

public class ServerRequestProcessor
{
    private readonly SqlServerRepository _repository;
    private readonly AnalysisRpcClient _analysisRpcClient;
    private readonly FileReceiver _fileReceiver;
    private readonly string _videoFolder;

    public ServerRequestProcessor(
        SqlServerRepository repository,
        AnalysisRpcClient analysisRpcClient,
        FileReceiver fileReceiver,
        string videoFolder)
    {
        _repository = repository;
        _analysisRpcClient = analysisRpcClient;
        _fileReceiver = fileReceiver;
        _videoFolder = videoFolder;
    }

    public void ProcessClient(TcpClient client)
    {
        try
        {
            using NetworkStream ns = client.GetStream();
            using StreamReader reader = new(ns);
            using StreamWriter writer = new(ns) { AutoFlush = true };

            string? line;
            while ((line = reader.ReadLine()) != null)
            {
                Console.WriteLine("[SERVIDOR] Recebido: " + line);

                string[] parts = line.Split('|');
                if (parts.Length == 0)
                {
                    writer.WriteLine("ERROR|mensagem_invalida");
                    continue;
                }

                string command = parts[0];

                if (command == "STORE" && parts.Length == 7)
                {
                    HandleStore(parts, writer);
                }
                else if (command == "VIDEO_UPLOAD" && parts.Length == 7)
                {
                    HandleVideoUpload(parts, writer, ns);
                }
                else
                {
                    writer.WriteLine("ERROR|mensagem_invalida");
                }
            }
        }
        catch (IOException ex)
        {
            Console.WriteLine("[SERVIDOR] Ligação terminada/timeout: " + ex.Message);
        }
        catch (Exception ex)
        {
            Console.WriteLine("[SERVIDOR] Erro: " + ex.Message);
        }
        finally
        {
            try { client.Close(); } catch { }
            Console.WriteLine("[SERVIDOR] Ligação terminada.");
        }
    }

    private void HandleStore(string[] parts, StreamWriter writer)
    {
        string timestampStr = parts[1].Trim();
        string sensorId = parts[2].Trim();
        string zona = parts[3].Trim();
        string tipo = parts[4].Trim();
        string valorStr = parts[5].Trim();
        string processedByGateway = parts[6].Trim();

        if (!DateTime.TryParse(timestampStr, out DateTime timestamp))
        {
            writer.WriteLine("STORE_ACK|ERROR|timestamp_invalido");
            return;
        }

        if (!double.TryParse(valorStr, NumberStyles.Float, CultureInfo.InvariantCulture, out double valor))
        {
            writer.WriteLine("STORE_ACK|ERROR|valor_invalido");
            return;
        }

        MeasurementRecord measurement = new()
        {
            Timestamp = timestamp,
            SensorId = sensorId,
            Zona = zona,
            Tipo = tipo,
            Valor = valor,
            ProcessedByGateway = processedByGateway
        };

        int measurementId = _repository.InsertMeasurement(measurement);
        AnalysisResponse analysis = _analysisRpcClient.Analyse(measurement);

        if (analysis.Ok)
        {
            _repository.InsertAnalysisResult(measurementId, measurement, analysis);
        }

        writer.WriteLine("STORE_ACK|OK");
    }

    private void HandleVideoUpload(string[] parts, StreamWriter writer, NetworkStream ns)
    {
        string timestampStr = parts[1].Trim();
        string sensorId = parts[2].Trim();
        string zona = parts[3].Trim();
        string originalFileName = parts[4].Trim();
        string processedByGateway = parts[6].Trim();

        if (!DateTime.TryParse(timestampStr, out DateTime timestamp))
        {
            writer.WriteLine("VIDEO_UPLOAD_ACK|ERROR|timestamp_invalido");
            return;
        }

        if (!long.TryParse(parts[5].Trim(), out long fileSize) || fileSize < 0)
        {
            writer.WriteLine("VIDEO_UPLOAD_ACK|ERROR|tamanho_invalido");
            return;
        }

        writer.WriteLine("VIDEO_UPLOAD_ACK|READY");
        writer.Flush();

        string safeFileName =
            $"{sensorId}_{DateTime.Now:yyyyMMdd_HHmmss}_{Path.GetFileName(originalFileName)}";

        string fullPath = Path.Combine(_videoFolder, safeFileName);

        bool ok = _fileReceiver.Receive(ns, fullPath, fileSize);

        if (!ok)
        {
            writer.WriteLine("VIDEO_UPLOAD_ACK|ERROR|falha_rececao_video");
            return;
        }

        VideoRecord video = new()
        {
            Timestamp = timestamp,
            SensorId = sensorId,
            Zona = zona,
            FileName = originalFileName,
            FilePath = fullPath,
            FileSize = fileSize,
            ProcessedByGateway = processedByGateway
        };

        _repository.InsertVideoEvent(video);
        writer.WriteLine("VIDEO_UPLOAD_ACK|OK");
    }
}

public class TcpServerHost
{
    private readonly int _port;
    private readonly ServerRequestProcessor _processor;

    public TcpServerHost(int port, ServerRequestProcessor processor)
    {
        _port = port;
        _processor = processor;
    }

    public void Run()
    {
        TcpListener listener = new(IPAddress.Any, _port);
        listener.Start();

        Console.WriteLine($"[SERVIDOR] À escuta na porta {_port}...");
        Console.WriteLine("[SERVIDOR] A usar SQL Server LocalDB.");

        while (true)
        {
            TcpClient client = listener.AcceptTcpClient();
            client.NoDelay = true;
            client.ReceiveTimeout = 30000;
            client.SendTimeout = 30000;

            Console.WriteLine("[SERVIDOR] Gateway ligado.");

            Thread clientThread = new(() => _processor.ProcessClient(client))
            {
                IsBackground = true
            };
            clientThread.Start();
        }
    }
}

public static class Program
{
    public static void Main(string[] args)
    {
        string connectionString =
            @"Server=(localdb)\MSSQLLocalDB;Database=MonitorizacaoUrbana;Trusted_Connection=True;TrustServerCertificate=True;";

        string videoFolder = "videos_recebidos";
        Directory.CreateDirectory(videoFolder);

        SqlServerRepository repository = new(connectionString);
        repository.EnsureTables();

        AnalysisRpcClient analysisRpcClient = new("127.0.0.1", 8000);
        FileReceiver fileReceiver = new();
        ServerRequestProcessor processor = new(repository, analysisRpcClient, fileReceiver, videoFolder);
        TcpServerHost host = new(6000, processor);

        Console.WriteLine("[SERVIDOR] Pasta de vídeos: " + Path.GetFullPath(videoFolder));
        host.Run();
    }
}