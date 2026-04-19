using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;

class GatewayApp
{
    static readonly object configLock = new object();
    static readonly object logLock = new object();
    static readonly object heartbeatDictLock = new object();

    static readonly string configFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "sensores_config.csv");
    static readonly string logFile = "gateway_log.txt";

    static readonly Dictionary<string, DateTime> lastHeartbeats =
        new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

    static readonly HashSet<string> inactiveAlerted =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    static void Main(string[] args)
    {
        int port = 5000;
        TcpListener listener = new TcpListener(IPAddress.Any, port);
        listener.Start();

        Console.WriteLine($"[GATEWAY] À escuta na porta {port}...");

        Thread heartbeatMonitor = new Thread(MonitorHeartbeats)
        {
            IsBackground = true
        };
        heartbeatMonitor.Start();

        while (true)
        {
            TcpClient client = listener.AcceptTcpClient();
            client.NoDelay = true;
            client.ReceiveTimeout = 30000;
            client.SendTimeout = 30000;

            Console.WriteLine("[GATEWAY] Sensor ligado.");

            Thread clientThread = new Thread(() => HandleSensor(client))
            {
                IsBackground = true
            };
            clientThread.Start();
        }
    }

    static void HandleSensor(TcpClient client)
    {
        string sessionSensorId = null;
        string sessionZona = null;
        bool helloDone = false;

        try
        {
            NetworkStream ns = client.GetStream();
            StreamReader reader = new StreamReader(ns);
            StreamWriter writer = new StreamWriter(ns) { AutoFlush = true };

            string line;
            while ((line = reader.ReadLine()) != null)
            {
                Console.WriteLine("[GATEWAY] Recebido: " + line);

                string[] parts = line.Split('|');
                if (parts.Length == 0)
                {
                    writer.WriteLine("ERROR|mensagem_invalida");
                    continue;
                }

                string command = parts[0];

                if (!helloDone)
                {
                    if (command != "HELLO")
                    {
                        writer.WriteLine("ERROR|hello_obrigatorio");
                        continue;
                    }

                    if (parts.Length != 4)
                    {
                        writer.WriteLine("HELLO_ACK|ERROR|mensagem_invalida");
                        continue;
                    }

                    string sensorId = parts[1].Trim();
                    string zona = parts[2].Trim();
                    string tipos = parts[3].Trim();

                    if (!SensorExiste(sensorId))
                    {
                        writer.WriteLine("HELLO_ACK|ERROR|sensor_nao_registado");
                        continue;
                    }

                    string estado = GetEstadoSensor(sensorId);
                    if (IsManutencao(estado))
                    {
                        writer.WriteLine("HELLO_ACK|ERROR|sensor_manutencao");
                        continue;
                    }

                    if (estado.Equals("desativado", StringComparison.OrdinalIgnoreCase))
                    {
                        writer.WriteLine("HELLO_ACK|ERROR|sensor_desativado");
                        continue;
                    }

                    string zonaConfig = GetZonaSensor(sensorId);
                    if (!zonaConfig.Equals(zona, StringComparison.OrdinalIgnoreCase))
                    {
                        writer.WriteLine("HELLO_ACK|ERROR|zona_invalida");
                        continue;
                    }

                    sessionSensorId = sensorId;
                    sessionZona = zona;
                    helloDone = true;

                    UpdateLastSync(sensorId);
                    UpdateHeartbeat(sensorId);
                    Log($"HELLO aceite de {sensorId} na zona {zona} com tipos {tipos}");

                    writer.WriteLine("HELLO_ACK|OK");
                    continue;
                }

                if (command == "DATA")
                {
                    if (parts.Length != 6)
                    {
                        writer.WriteLine("DATA_ACK|ERROR|mensagem_invalida");
                        continue;
                    }

                    string timestamp = parts[1].Trim();
                    string sensorId = parts[2].Trim();
                    string zona = parts[3].Trim();
                    string tipo = parts[4].Trim();
                    string valor = parts[5].Trim();

                    if (!ValidateSession(writer, sessionSensorId, sessionZona, sensorId, zona, "DATA_ACK"))
                        continue;

                    if (!SensorExiste(sensorId))
                    {
                        writer.WriteLine("DATA_ACK|ERROR|sensor_nao_registado");
                        continue;
                    }

                    if (!SensorAtivo(sensorId))
                    {
                        writer.WriteLine("DATA_ACK|ERROR|sensor_indisponivel");
                        continue;
                    }

                    if (!TipoSuportado(sensorId, tipo))
                    {
                        writer.WriteLine("DATA_ACK|ERROR|tipo_nao_suportado");
                        continue;
                    }

                    UpdateLastSync(sensorId);
                    UpdateHeartbeat(sensorId);
                    Log($"DATA de {sensorId}: {tipo}={valor}");

                    bool ok = EncaminharParaServidorTexto($"STORE|{timestamp}|{sensorId}|{zona}|{tipo}|{valor}");
                    writer.WriteLine(ok ? "DATA_ACK|OK" : "DATA_ACK|ERROR|falha_no_servidor");
                }
                else if (command == "HEARTBEAT")
                {
                    if (parts.Length != 3)
                    {
                        writer.WriteLine("HEARTBEAT_ACK|ERROR|mensagem_invalida");
                        continue;
                    }

                    string timestamp = parts[1].Trim();
                    string sensorId = parts[2].Trim();

                    if (sessionSensorId == null || !sessionSensorId.Equals(sensorId, StringComparison.OrdinalIgnoreCase))
                    {
                        writer.WriteLine("HEARTBEAT_ACK|ERROR|sessao_invalida");
                        continue;
                    }

                    if (!SensorExiste(sensorId))
                    {
                        writer.WriteLine("HEARTBEAT_ACK|ERROR|sensor_nao_registado");
                        continue;
                    }

                    UpdateLastSync(sensorId);
                    UpdateHeartbeat(sensorId);
                    Log($"HEARTBEAT de {sensorId} em {timestamp}");

                    writer.WriteLine("HEARTBEAT_ACK|OK");
                }
                else if (command == "VIDEO_REQUEST")
                {
                    if (parts.Length != 6)
                    {
                        writer.WriteLine("VIDEO_ACK|ERROR|mensagem_invalida");
                        continue;
                    }

                    string timestamp = parts[1].Trim();
                    string sensorId = parts[2].Trim();
                    string zona = parts[3].Trim();
                    string fileName = parts[4].Trim();

                    if (!long.TryParse(parts[5].Trim(), out long fileSize) || fileSize < 0)
                    {
                        writer.WriteLine("VIDEO_ACK|ERROR|tamanho_invalido");
                        continue;
                    }

                    if (!ValidateSession(writer, sessionSensorId, sessionZona, sensorId, zona, "VIDEO_ACK"))
                        continue;

                    if (!SensorExiste(sensorId))
                    {
                        writer.WriteLine("VIDEO_ACK|ERROR|sensor_nao_registado");
                        continue;
                    }

                    if (!SensorAtivo(sensorId))
                    {
                        writer.WriteLine("VIDEO_ACK|ERROR|sensor_indisponivel");
                        continue;
                    }

                    if (!TipoSuportado(sensorId, "VIDEO"))
                    {
                        writer.WriteLine("VIDEO_ACK|ERROR|tipo_nao_suportado");
                        continue;
                    }

                    UpdateLastSync(sensorId);
                    UpdateHeartbeat(sensorId);
                    Log($"VIDEO_REQUEST de {sensorId}: {fileName} ({fileSize} bytes)");

                    writer.WriteLine("VIDEO_ACK|READY");
                    writer.Flush();

                    byte[] videoBytes = ReceiveBytes(ns, fileSize);
                    if (videoBytes == null || videoBytes.Length != fileSize)
                    {
                        writer.WriteLine("VIDEO_ACK|ERROR|falha_rececao_video");
                        continue;
                    }

                    bool ok = EncaminharVideoParaServidor(timestamp, sensorId, zona, fileName, videoBytes);

                    writer.WriteLine(ok
                        ? "VIDEO_ACK|OK|video_encaminhado"
                        : "VIDEO_ACK|ERROR|falha_no_servidor");
                }
                else if (command == "BYE")
                {
                    if (parts.Length != 2)
                    {
                        writer.WriteLine("BYE_ACK|ERROR|mensagem_invalida");
                        continue;
                    }

                    string sensorId = parts[1].Trim();

                    if (sessionSensorId == null || !sessionSensorId.Equals(sensorId, StringComparison.OrdinalIgnoreCase))
                    {
                        writer.WriteLine("BYE_ACK|ERROR|sessao_invalida");
                        continue;
                    }

                    Log($"BYE de {sensorId}");
                    writer.WriteLine("BYE_ACK|OK");
                    break;
                }
                else
                {
                    writer.WriteLine("ERROR|mensagem_invalida");
                }
            }
        }
        catch (IOException ex)
        {
            Log("Ligação terminada/timeout com sensor: " + ex.Message);
            Console.WriteLine("[GATEWAY] Ligação terminada/timeout: " + ex.Message);
        }
        catch (Exception ex)
        {
            Log("Erro no gateway: " + ex.Message);
            Console.WriteLine("[GATEWAY] Erro: " + ex.Message);
        }
        finally
        {
            try { client.Close(); } catch { }
            Console.WriteLine("[GATEWAY] Ligação terminada.");
        }
    }

    static byte[] ReceiveBytes(NetworkStream ns, long totalBytes)
    {
        try
        {
            byte[] buffer = new byte[8192];
            long totalRead = 0;

            MemoryStream ms = new MemoryStream();

            while (totalRead < totalBytes)
            {
                int toRead = (int)Math.Min(buffer.Length, totalBytes - totalRead);
                int bytesRead = ns.Read(buffer, 0, toRead);

                if (bytesRead <= 0)
                    return null;

                ms.Write(buffer, 0, bytesRead);
                totalRead += bytesRead;
            }

            return ms.ToArray();
        }
        catch
        {
            return null;
        }
    }

    static bool EncaminharVideoParaServidor(string timestamp, string sensorId, string zona, string fileName, byte[] videoBytes)
    {
        try
        {
            TcpClient serverClient = new TcpClient("127.0.0.1", 6000);
            serverClient.NoDelay = true;
            serverClient.ReceiveTimeout = 30000;
            serverClient.SendTimeout = 30000;

            NetworkStream ns = serverClient.GetStream();
            StreamReader reader = new StreamReader(ns);
            StreamWriter writer = new StreamWriter(ns) { AutoFlush = true };

            string header = $"VIDEO_UPLOAD|{timestamp}|{sensorId}|{zona}|{fileName}|{videoBytes.Length}";
            writer.WriteLine(header);
            writer.Flush();

            string ready = reader.ReadLine();
            if (ready == null || !ready.Equals("VIDEO_UPLOAD_ACK|READY", StringComparison.OrdinalIgnoreCase))
            {
                Log($"Servidor não ficou pronto para vídeo de {sensorId}. Resposta: {ready}");
                return false;
            }

            ns.Write(videoBytes, 0, videoBytes.Length);
            ns.Flush();

            string finalAck = reader.ReadLine();
            Log($"Vídeo encaminhado ao servidor: {header} | Resposta: {finalAck}");

            return finalAck != null && finalAck.Equals("VIDEO_UPLOAD_ACK|OK", StringComparison.OrdinalIgnoreCase);
        }
        catch (Exception ex)
        {
            Log("Erro ao encaminhar vídeo ao servidor: " + ex.Message);
            return false;
        }
    }

    static bool EncaminharParaServidorTexto(string mensagem)
    {
        try
        {
            TcpClient serverClient = new TcpClient("127.0.0.1", 6000);
            serverClient.NoDelay = true;
            serverClient.ReceiveTimeout = 15000;
            serverClient.SendTimeout = 15000;

            NetworkStream ns = serverClient.GetStream();
            StreamWriter writer = new StreamWriter(ns) { AutoFlush = true };
            StreamReader reader = new StreamReader(ns);

            writer.WriteLine(mensagem);
            string response = reader.ReadLine();

            Log($"Encaminhado ao servidor: {mensagem} | Resposta: {response}");

            return response != null && response.IndexOf("OK", StringComparison.OrdinalIgnoreCase) >= 0;
        }
        catch (Exception ex)
        {
            Log("Erro ao ligar ao servidor: " + ex.Message);
            return false;
        }
    }

    static bool ValidateSession(
        StreamWriter writer,
        string sessionSensorId,
        string sessionZona,
        string sensorId,
        string zona,
        string ackPrefix)
    {
        if (sessionSensorId == null || sessionZona == null)
        {
            writer.WriteLine($"{ackPrefix}|ERROR|sessao_invalida");
            return false;
        }

        if (!sessionSensorId.Equals(sensorId, StringComparison.OrdinalIgnoreCase))
        {
            writer.WriteLine($"{ackPrefix}|ERROR|sensor_id_invalido");
            return false;
        }

        if (!sessionZona.Equals(zona, StringComparison.OrdinalIgnoreCase))
        {
            writer.WriteLine($"{ackPrefix}|ERROR|zona_invalida");
            return false;
        }

        return true;
    }

    static bool SensorExiste(string sensorId)
    {
        lock (configLock)
        {
            if (!File.Exists(configFile))
                return false;

            foreach (string line in File.ReadAllLines(configFile))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;

                string[] parts = line.Split(':');
                if (parts.Length >= 5 && parts[0].Trim().Equals(sensorId, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }
    }

    static string GetEstadoSensor(string sensorId)
    {
        lock (configLock)
        {
            if (!File.Exists(configFile))
                return "";

            foreach (string line in File.ReadAllLines(configFile))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;

                string[] parts = line.Split(':');
                if (parts.Length >= 5 && parts[0].Trim().Equals(sensorId, StringComparison.OrdinalIgnoreCase))
                    return parts[1].Trim();
            }

            return "";
        }
    }

    static string GetZonaSensor(string sensorId)
    {
        lock (configLock)
        {
            if (!File.Exists(configFile))
                return "";

            foreach (string line in File.ReadAllLines(configFile))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;

                string[] parts = line.Split(':');
                if (parts.Length >= 5 && parts[0].Trim().Equals(sensorId, StringComparison.OrdinalIgnoreCase))
                    return parts[2].Trim();
            }

            return "";
        }
    }

    static bool IsManutencao(string estado)
    {
        return estado.Equals("manutencao", StringComparison.OrdinalIgnoreCase) ||
               estado.Equals("manutenção", StringComparison.OrdinalIgnoreCase);
    }

    static bool SensorAtivo(string sensorId)
    {
        string estado = GetEstadoSensor(sensorId);

        if (estado.Equals("desativado", StringComparison.OrdinalIgnoreCase))
            return false;

        if (IsManutencao(estado))
            return false;

        return estado.Equals("ativo", StringComparison.OrdinalIgnoreCase);
    }

    static bool TipoSuportado(string sensorId, string tipo)
    {
        lock (configLock)
        {
            if (!File.Exists(configFile))
                return false;

            foreach (string line in File.ReadAllLines(configFile))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;

                string[] parts = line.Split(':');

                if (parts.Length >= 5 && parts[0].Trim().Equals(sensorId, StringComparison.OrdinalIgnoreCase))
                {
                    string tiposPart = parts[3].Trim().Replace("[", "").Replace("]", "");
                    string[] tipos = tiposPart.Split(',');

                    foreach (string t in tipos)
                    {
                        if (t.Trim().Equals(tipo, StringComparison.OrdinalIgnoreCase))
                            return true;
                    }

                    return false;
                }
            }

            return false;
        }
    }

    static void UpdateLastSync(string sensorId)
    {
        lock (configLock)
        {
            if (!File.Exists(configFile))
                return;

            string[] lines = File.ReadAllLines(configFile);

            for (int i = 0; i < lines.Length; i++)
            {
                if (string.IsNullOrWhiteSpace(lines[i])) continue;

                string[] parts = lines[i].Split(':');
                if (parts.Length >= 5 && parts[0].Trim().Equals(sensorId, StringComparison.OrdinalIgnoreCase))
                {
                    parts[4] = DateTime.Now.ToString("s");
                    lines[i] = string.Join(":", parts);
                    break;
                }
            }

            File.WriteAllLines(configFile, lines);
        }
    }

    static void Log(string message)
    {
        lock (logLock)
        {
            string line = $"{DateTime.Now:s} {message}";
            File.AppendAllText(logFile, line + Environment.NewLine);
        }
    }

    static void UpdateHeartbeat(string sensorId)
    {
        lock (heartbeatDictLock)
        {
            lastHeartbeats[sensorId] = DateTime.Now;
            inactiveAlerted.Remove(sensorId);
        }
    }

    static void MonitorHeartbeats()
    {
        while (true)
        {
            Thread.Sleep(10000);

            List<string> sensoresInativos = new List<string>();

            lock (heartbeatDictLock)
            {
                foreach (var kvp in lastHeartbeats)
                {
                    TimeSpan diff = DateTime.Now - kvp.Value;
                    if (diff.TotalSeconds > 30 && !inactiveAlerted.Contains(kvp.Key))
                    {
                        sensoresInativos.Add(kvp.Key);
                        inactiveAlerted.Add(kvp.Key);
                    }
                }
            }

            foreach (string sensorId in sensoresInativos)
            {
                Log($"ALERTA: Sensor {sensorId} sem heartbeat há mais de 30 segundos");
                Console.WriteLine($"[GATEWAY] ALERTA: Sensor {sensorId} inativo");
            }
        }
    }
}