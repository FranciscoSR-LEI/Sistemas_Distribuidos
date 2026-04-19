using System;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Data.SqlClient;

class Program
{
    static readonly string connectionString =
        @"Server=(localdb)\MSSQLLocalDB;Database=MonitorizacaoUrbana;Trusted_Connection=True;TrustServerCertificate=True;";

    static readonly string videoFolder = "videos_recebidos";
    static readonly object dbLock = new object();

    static void Main(string[] args)
    {
        Directory.CreateDirectory(videoFolder);

        int port = 6000;
        TcpListener listener = new TcpListener(IPAddress.Any, port);
        listener.Start();

        Console.WriteLine($"[SERVIDOR] À escuta na porta {port}...");
        Console.WriteLine("[SERVIDOR] A usar SQL Server LocalDB.");
        Console.WriteLine("[SERVIDOR] Pasta de vídeos: " + Path.GetFullPath(videoFolder));

        while (true)
        {
            TcpClient client = listener.AcceptTcpClient();
            client.NoDelay = true;
            client.ReceiveTimeout = 30000;
            client.SendTimeout = 30000;

            Console.WriteLine("[SERVIDOR] Gateway ligado.");

            Thread clientThread = new Thread(() => HandleGateway(client))
            {
                IsBackground = true
            };
            clientThread.Start();
        }
    }

    static void HandleGateway(TcpClient client)
    {
        try
        {
            NetworkStream ns = client.GetStream();
            StreamReader reader = new StreamReader(ns);
            StreamWriter writer = new StreamWriter(ns) { AutoFlush = true };

            string line;
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

                if (command == "STORE" && parts.Length == 6)
                {
                    string timestampStr = parts[1].Trim();
                    string sensorId = parts[2].Trim();
                    string zona = parts[3].Trim();
                    string tipo = parts[4].Trim();
                    string valorStr = parts[5].Trim();

                    if (!DateTime.TryParse(timestampStr, out DateTime timestamp))
                    {
                        writer.WriteLine("STORE_ACK|ERROR|timestamp_invalido");
                        continue;
                    }

                    if (!double.TryParse(valorStr, NumberStyles.Float, CultureInfo.InvariantCulture, out double valor))
                    {
                        writer.WriteLine("STORE_ACK|ERROR|valor_invalido");
                        continue;
                    }

                    InsertMeasurement(timestamp, sensorId, zona, tipo, valor);
                    writer.WriteLine("STORE_ACK|OK");
                }
                else if (command == "VIDEO_UPLOAD" && parts.Length == 6)
                {
                    string timestampStr = parts[1].Trim();
                    string sensorId = parts[2].Trim();
                    string zona = parts[3].Trim();
                    string originalFileName = parts[4].Trim();

                    if (!DateTime.TryParse(timestampStr, out DateTime timestamp))
                    {
                        writer.WriteLine("VIDEO_UPLOAD_ACK|ERROR|timestamp_invalido");
                        continue;
                    }

                    if (!long.TryParse(parts[5].Trim(), out long fileSize) || fileSize < 0)
                    {
                        writer.WriteLine("VIDEO_UPLOAD_ACK|ERROR|tamanho_invalido");
                        continue;
                    }

                    writer.WriteLine("VIDEO_UPLOAD_ACK|READY");
                    writer.Flush();

                    string safeFileName =
                        $"{sensorId}_{DateTime.Now:yyyyMMdd_HHmmss}_{Path.GetFileName(originalFileName)}";

                    string fullPath = Path.Combine(videoFolder, safeFileName);

                    bool ok = ReceiveFile(ns, fullPath, fileSize);

                    if (ok)
                    {
                        InsertVideoEvent(timestamp, sensorId, zona, originalFileName, fullPath, fileSize);
                        writer.WriteLine("VIDEO_UPLOAD_ACK|OK");
                    }
                    else
                    {
                        writer.WriteLine("VIDEO_UPLOAD_ACK|ERROR|falha_rececao_video");
                    }
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

    static void InsertMeasurement(DateTime timestamp, string sensorId, string zona, string tipo, double valor)
    {
        lock (dbLock)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();

            string sql = @"
                INSERT INTO Measurements ([Timestamp], SensorId, Zona, Tipo, Valor)
                VALUES (@Timestamp, @SensorId, @Zona, @Tipo, @Valor);";

            SqlCommand cmd = new SqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Timestamp", timestamp);
            cmd.Parameters.AddWithValue("@SensorId", sensorId);
            cmd.Parameters.AddWithValue("@Zona", zona);
            cmd.Parameters.AddWithValue("@Tipo", tipo);
            cmd.Parameters.AddWithValue("@Valor", valor);

            cmd.ExecuteNonQuery();
        }
    }

    static void InsertVideoEvent(DateTime timestamp, string sensorId, string zona, string fileName, string filePath, long fileSize)
    {
        lock (dbLock)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();

            string sql = @"
                INSERT INTO VideoEvents ([Timestamp], SensorId, Zona, FileName, FilePath, FileSize)
                VALUES (@Timestamp, @SensorId, @Zona, @FileName, @FilePath, @FileSize);";

            SqlCommand cmd = new SqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Timestamp", timestamp);
            cmd.Parameters.AddWithValue("@SensorId", sensorId);
            cmd.Parameters.AddWithValue("@Zona", zona);
            cmd.Parameters.AddWithValue("@FileName", fileName);
            cmd.Parameters.AddWithValue("@FilePath", filePath);
            cmd.Parameters.AddWithValue("@FileSize", fileSize);

            cmd.ExecuteNonQuery();
        }
    }

    static bool ReceiveFile(NetworkStream ns, string fullPath, long totalBytes)
    {
        try
        {
            byte[] buffer = new byte[8192];
            long totalRead = 0;

            FileStream fs = new FileStream(fullPath, FileMode.Create, FileAccess.Write);

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