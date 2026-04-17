using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;

class GatewayApp
{
    // Substituição dos object locks por Mutexes para cumprir os requisitos da Fase 4
    static Mutex csvMutex = new Mutex();
    static Mutex logMutex = new Mutex();
    static readonly object heartbeatDictLock = new object(); // Mantém-se lock simples para variáveis em memória

    static string configFile = "sensores_config.csv";
    static string logFile = "gateway_log.txt";

    static Dictionary<string, DateTime> lastHeartbeats = new Dictionary<string, DateTime>();

    static void Main(string[] args)
    {
        int port = 5000;
        TcpListener listener = new TcpListener(IPAddress.Any, port);

        // Garantir que o ficheiro existe logo no arranque
        if (!File.Exists(configFile))
        {
            File.WriteAllText(configFile, "S101:ativo:ZONA CENTRO:[TEMP,HUM,RUIDO]:2026-03-10T08:45:00\nS102:ativo:ZONA ESCOLAR:[PM2.5,TEMP]:2026-03-10T09:00:00\n");
        }

        listener.Start();
        Console.WriteLine($"[GATEWAY] À escuta na porta {port}...");

        Thread heartbeatMonitor = new Thread(MonitorHeartbeats);
        heartbeatMonitor.Start();

        while (true)
        {
            TcpClient client = listener.AcceptTcpClient();
            Console.WriteLine("[GATEWAY] Novo sensor ligado.");

            // Fase 4 - Atendimento concorrente através de threads
            Thread clientThread = new Thread(() => HandleSensor(client));
            clientThread.Start();
        }
    }

    static void HandleSensor(TcpClient client)
    {
        string sensorId = "";

        try
        {
            using NetworkStream ns = client.GetStream();
            using StreamReader reader = new StreamReader(ns);
            using StreamWriter writer = new StreamWriter(ns) { AutoFlush = true };

            string line;
            while ((line = reader.ReadLine()) != null)
            {
                Console.WriteLine("[GATEWAY] Recebido: " + line);
                string[] parts = line.Split('|');
                string command = parts[0];

                if (command == "HELLO" && parts.Length >= 4)
                {
                    sensorId = parts[1];
                    string zona = parts[2];
                    string tipos = parts[3];

                    string estado = GetEstadoSensor(sensorId);

                    if (estado == null)
                    {
                        writer.WriteLine("HELLO_ACK|ERROR|sensor_nao_registado");
                        continue;
                    }
                    if (estado == "manutencao")
                    {
                        writer.WriteLine("HELLO_ACK|ERROR|sensor_manutencao");
                        continue;
                    }
                    if (estado == "desativado")
                    {
                        writer.WriteLine("HELLO_ACK|ERROR|sensor_desativado");
                        continue;
                    }

                    UpdateLastSync(sensorId);
                    UpdateHeartbeat(sensorId);
                    Log($"HELLO aceite de {sensorId} na zona {zona} com tipos {tipos}");

                    writer.WriteLine("HELLO_ACK|OK");
                }
                else if (command == "DATA" && parts.Length >= 6)
                {
                    string timestamp = parts[1];
                    sensorId = parts[2];
                    string zona = parts[3];
                    string tipo = parts[4];
                    string valor = parts[5];

                    if (GetEstadoSensor(sensorId) == null)
                    {
                        writer.WriteLine("DATA_ACK|ERROR|sensor_nao_registado");
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

                    bool ok = EncaminharParaServidor($"STORE|{timestamp}|{sensorId}|{zona}|{tipo}|{valor}");
                    writer.WriteLine(ok ? "DATA_ACK|OK" : "DATA_ACK|ERROR|falha_no_servidor");
                }
                else if (command == "HEARTBEAT" && parts.Length >= 3)
                {
                    sensorId = parts[2];
                    UpdateLastSync(sensorId);
                    UpdateHeartbeat(sensorId);
                    Log($"HEARTBEAT de {sensorId}");

                    writer.WriteLine("HEARTBEAT_ACK|OK");
                }
                else if (command == "VIDEO_REQUEST" && parts.Length >= 4)
                {
                    string timestamp = parts[1];
                    sensorId = parts[2];
                    string zona = parts[3];

                    UpdateLastSync(sensorId);
                    UpdateHeartbeat(sensorId);
                    Log($"VIDEO_REQUEST de {sensorId}");

                    bool ok = EncaminharParaServidor($"VIDEO_EVENT|{timestamp}|{sensorId}|{zona}|pedido_stream");
                    writer.WriteLine(ok ? "VIDEO_ACK|OK" : "VIDEO_ACK|ERROR|falha_no_servidor");
                }
                else if (command == "BYE" && parts.Length >= 2)
                {
                    sensorId = parts[1];
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
        catch (Exception ex)
        {
            Console.WriteLine($"[GATEWAY] Erro com {sensorId}: " + ex.Message);
        }
        finally
        {
            client.Close();
            Console.WriteLine($"[GATEWAY] Ligação terminada com o sensor {sensorId}.");
        }
    }

    // Retorna o estado ou null se não existir
    static string GetEstadoSensor(string sensorId)
    {
        csvMutex.WaitOne();
        try
        {
            if (!File.Exists(configFile)) return null;

            string[] lines = File.ReadAllLines(configFile);
            foreach (string line in lines)
            {
                // Limitar o split a 5 partes para não quebrar o timestamp!
                string[] parts = line.Split(new char[] { ':' }, 5);
                if (parts[0] == sensorId)
                    return parts[1];
            }
            return null;
        }
        finally
        {
            csvMutex.ReleaseMutex();
        }
    }

    static bool TipoSuportado(string sensorId, string tipo)
    {
        csvMutex.WaitOne();
        try
        {
            string[] lines = File.ReadAllLines(configFile);
            foreach (string line in lines)
            {
                string[] parts = line.Split(new char[] { ':' }, 5);
                if (parts[0] == sensorId)
                {
                    string tiposPart = parts[3]; // ex: [PM2.5,TEMP,RUIDO]
                    tiposPart = tiposPart.Replace("[", "").Replace("]", "");
                    string[] tipos = tiposPart.Split(',');

                    foreach (string t in tipos)
                    {
                        if (t.Trim().Equals(tipo, StringComparison.OrdinalIgnoreCase))
                            return true;
                    }
                }
            }
            return false;
        }
        finally
        {
            csvMutex.ReleaseMutex();
        }
    }

    static void UpdateLastSync(string sensorId)
    {
        csvMutex.WaitOne();
        try
        {
            string[] lines = File.ReadAllLines(configFile);
            for (int i = 0; i < lines.Length; i++)
            {
                string[] parts = lines[i].Split(new char[] { ':' }, 5);
                if (parts[0] == sensorId)
                {
                    parts[4] = DateTime.Now.ToString("s"); // Atualiza o timestamp
                    lines[i] = string.Join(":", parts);
                    break; // Sai do ciclo após encontrar e atualizar
                }
            }
            File.WriteAllLines(configFile, lines);
        }
        finally
        {
            csvMutex.ReleaseMutex();
        }
    }

    static void Log(string message)
    {
        logMutex.WaitOne();
        try
        {
            string line = $"{DateTime.Now:s} {message}";
            File.AppendAllText(logFile, line + Environment.NewLine);
        }
        finally
        {
            logMutex.ReleaseMutex();
        }
    }

    static bool EncaminharParaServidor(string mensagem)
    {
        try
        {
            using TcpClient serverClient = new TcpClient("127.0.0.1", 6000);
            using NetworkStream ns = serverClient.GetStream();
            using StreamWriter writer = new StreamWriter(ns) { AutoFlush = true };
            using StreamReader reader = new StreamReader(ns);

            writer.WriteLine(mensagem);
            string response = reader.ReadLine();

            Log($"Encaminhado ao servidor: {mensagem} | Resposta: {response}");

            return response != null && response.Contains("OK");
        }
        catch (Exception ex)
        {
            Log("Erro ao ligar ao servidor: " + ex.Message);
            return false;
        }
    }

    static void UpdateHeartbeat(string sensorId)
    {
        lock (heartbeatDictLock)
        {
            lastHeartbeats[sensorId] = DateTime.Now;
        }
    }

    static void MonitorHeartbeats()
    {
        while (true)
        {
            Thread.Sleep(10000); // verifica de 10 em 10 segundos

            List<string> sensoresInativos = new List<string>();

            lock (heartbeatDictLock)
            {
                foreach (var kvp in lastHeartbeats)
                {
                    TimeSpan diff = DateTime.Now - kvp.Value;
                    if (diff.TotalSeconds > 30)
                    {
                        sensoresInativos.Add(kvp.Key);
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