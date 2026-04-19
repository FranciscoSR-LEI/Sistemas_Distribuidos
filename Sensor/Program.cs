using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net.Sockets;
using System.Threading;

class Program
{
    static string sensorId = "";
    static string zona = "ZONA_ESCOLAR";
    static List<string> tipos = new List<string>();

    static readonly string videoFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Video_demo.mp4");

    static TcpClient client;
    static NetworkStream ns;
    static StreamReader reader;
    static StreamWriter writer;

    static readonly object commLock = new object();
    static readonly Random rnd = new Random();

    static volatile bool running = true;
    static volatile bool transferringVideo = false;

    static Thread heartbeatThread;
    static Thread autoDataThread;

    static void Main(string[] args)
    {
        Console.CancelKeyPress += OnCancelKeyPress;

        Console.Write("Introduza o sensorId (0 a 50): ");
        string input = Console.ReadLine()?.Trim();

        if (!int.TryParse(input, out int sensorNumber) || sensorNumber < 0 || sensorNumber > 50)
        {
            Console.WriteLine("[SENSOR] sensorId inválido.");
            return;
        }

        sensorId = sensorNumber.ToString();
        string tipoAutomatico = GetAutomaticType(sensorNumber);
        tipos = GetTipos(sensorNumber);

        string gatewayIp = args.Length > 0 ? args[0] : "127.0.0.1";
        int gatewayPort = 5000;

        try
        {
            client = new TcpClient();
            client.NoDelay = true;
            client.ReceiveTimeout = 30000;
            client.SendTimeout = 30000;

            Console.WriteLine($"[SENSOR] A ligar ao gateway {gatewayIp}:{gatewayPort}...");
            client.Connect(gatewayIp, gatewayPort);

            ns = client.GetStream();
            reader = new StreamReader(ns);
            writer = new StreamWriter(ns) { AutoFlush = true };

            string hello = $"HELLO|{sensorId}|{zona}|{string.Join(",", tipos)}";
            string response = SendAndReceive(hello);

            Console.WriteLine("[SENSOR] Resposta ao HELLO: [" + (response ?? "null") + "]");

            if (response == null || !response.Trim().Equals("HELLO_ACK|OK", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine("[SENSOR] Ligação rejeitada pelo gateway.");
                return;
            }

            Console.WriteLine("[SENSOR] HELLO aceite.");

            heartbeatThread = new Thread(SendAutomaticHeartbeat)
            {
                IsBackground = true
            };
            heartbeatThread.Start();

            if (tipoAutomatico != null)
            {
                autoDataThread = new Thread(() => SendAutomaticData(tipoAutomatico))
                {
                    IsBackground = true
                };
                autoDataThread.Start();

                Console.WriteLine($"[SENSOR] Modo automático ativo para tipo: {tipoAutomatico}");
                Console.WriteLine("[SENSOR] O sensor vai continuar a enviar automaticamente. Termine com Ctrl+C.");

                while (running)
                {
                    Thread.Sleep(1000);
                }
            }
            else
            {
                Console.WriteLine("[SENSOR] Sensor de vídeo. Use Ctrl+C para terminar.");

                while (running)
                {
                    Console.WriteLine();
                    Console.WriteLine("--- MENU SENSOR VIDEO ---");
                    Console.WriteLine("1 - Enviar vídeo manual");
                    Console.WriteLine("2 - Enviar heartbeat manual");
                    Console.WriteLine("3 - Terminar");
                    Console.Write("Escolha: ");

                    string option = Console.ReadLine();

                    switch (option)
                    {
                        case "1":
                            SendRealVideo();
                            break;
                        case "2":
                            SendHeartbeat();
                            break;
                        case "3":
                            TerminateCommunication();
                            break;
                        default:
                            Console.WriteLine("Opção inválida.");
                            break;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("[SENSOR] Erro: " + ex.Message);
        }
        finally
        {
            running = false;

            try { heartbeatThread?.Join(1000); } catch { }
            try { autoDataThread?.Join(1000); } catch { }

            try { reader?.Close(); } catch { }
            try { writer?.Close(); } catch { }
            try { ns?.Close(); } catch { }
            try { client?.Close(); } catch { }
        }
    }

    static void OnCancelKeyPress(object sender, ConsoleCancelEventArgs e)
    {
        e.Cancel = true;
        Console.WriteLine("\n[SENSOR] A terminar...");
        TerminateCommunication();
    }

    static string GetAutomaticType(int sensorNumber)
    {
        if (sensorNumber >= 0 && sensorNumber <= 10) return "TEMP";
        if (sensorNumber >= 11 && sensorNumber <= 20) return "AR";
        if (sensorNumber >= 21 && sensorNumber <= 30) return "RUIDO";
        if (sensorNumber >= 31 && sensorNumber <= 40) return "HUM";
        return null;
    }

    static List<string> GetTipos(int sensorNumber)
    {
        if (sensorNumber >= 0 && sensorNumber <= 10) return new List<string> { "TEMP" };
        if (sensorNumber >= 11 && sensorNumber <= 20) return new List<string> { "AR" };
        if (sensorNumber >= 21 && sensorNumber <= 30) return new List<string> { "RUIDO" };
        if (sensorNumber >= 31 && sensorNumber <= 40) return new List<string> { "HUM" };
        return new List<string> { "VIDEO" };
    }

    static string GenerateRandomValue(string tipo)
    {
        lock (rnd)
        {
            if (tipo == "TEMP")
                return (20 + rnd.NextDouble() * 10).ToString("F2", CultureInfo.InvariantCulture);

            if (tipo == "AR")
                return rnd.Next(10, 101).ToString(CultureInfo.InvariantCulture);

            if (tipo == "RUIDO")
                return rnd.Next(30, 101).ToString(CultureInfo.InvariantCulture);

            if (tipo == "HUM")
                return rnd.Next(30, 91).ToString(CultureInfo.InvariantCulture);

            return "0";
        }
    }

    static string SendAndReceive(string message)
    {
        lock (commLock)
        {
            if (writer == null || reader == null)
                throw new InvalidOperationException("Comunicação não inicializada.");

            writer.WriteLine(message);
            return reader.ReadLine();
        }
    }

    static void SendAutomaticData(string tipo)
    {
        while (running)
        {
            try
            {
                Thread.Sleep(10000);

                if (!running)
                    break;

                if (transferringVideo)
                    continue;

                string valor = GenerateRandomValue(tipo);
                string timestamp = DateTime.Now.ToString("s");
                string message = $"DATA|{timestamp}|{sensorId}|{zona}|{tipo}|{valor}";

                string response = SendAndReceive(message);
                Console.WriteLine($"[SENSOR] Envio automático {tipo}={valor} -> {response}");
            }
            catch (Exception ex)
            {
                Console.WriteLine("[SENSOR] Erro no envio automático: " + ex.Message);
                running = false;
                break;
            }
        }
    }

    static void SendHeartbeat()
    {
        try
        {
            lock (commLock)
            {
                if (writer == null || reader == null)
                    throw new InvalidOperationException("Comunicação não inicializada.");

                if (transferringVideo)
                {
                    Console.WriteLine("[SENSOR] Heartbeat adiado: transferência de vídeo em curso.");
                    return;
                }

                string timestamp = DateTime.Now.ToString("s");
                string message = $"HEARTBEAT|{timestamp}|{sensorId}";

                writer.WriteLine(message);
                string response = reader.ReadLine();

                Console.WriteLine("[SENSOR] Resposta: " + response);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("[SENSOR] Erro ao enviar heartbeat: " + ex.Message);
            running = false;
        }
    }

    static void SendRealVideo()
    {
        try
        {
            lock (commLock)
            {
                if (writer == null || reader == null || ns == null)
                    throw new InvalidOperationException("Comunicação não inicializada.");

                if (!File.Exists(videoFile))
                {
                    Console.WriteLine("[SENSOR] Ficheiro de vídeo não encontrado: " + Path.GetFullPath(videoFile));
                    return;
                }

                transferringVideo = true;

                FileInfo fi = new FileInfo(videoFile);
                string fileName = fi.Name;
                long fileSize = fi.Length;
                string timestamp = DateTime.Now.ToString("s");

                string request = $"VIDEO_REQUEST|{timestamp}|{sensorId}|{zona}|{fileName}|{fileSize}";
                writer.WriteLine(request);
                writer.Flush();

                string ack = reader.ReadLine();
                Console.WriteLine("[SENSOR] Resposta: " + ack);

                if (ack == null || !ack.Trim().Equals("VIDEO_ACK|READY", StringComparison.OrdinalIgnoreCase))
                {
                    transferringVideo = false;
                    return;
                }

                byte[] buffer = new byte[8192];
                long totalSent = 0;

                FileStream fs = new FileStream(videoFile, FileMode.Open, FileAccess.Read);
                int bytesRead;
                while ((bytesRead = fs.Read(buffer, 0, buffer.Length)) > 0)
                {
                    ns.Write(buffer, 0, bytesRead);
                    totalSent += bytesRead;
                    Console.WriteLine($"[SENSOR] A enviar vídeo: {totalSent}/{fileSize} bytes");
                }

                ns.Flush();

                string finalAck = reader.ReadLine();
                Console.WriteLine("[SENSOR] Resposta final: " + finalAck);

                transferringVideo = false;
            }
        }
        catch (Exception ex)
        {
            transferringVideo = false;
            Console.WriteLine("[SENSOR] Erro ao enviar vídeo: " + ex.Message);
            running = false;
        }
    }

    static void TerminateCommunication()
    {
        try
        {
            if (!running) return;

            string message = $"BYE|{sensorId}";
            string response = SendAndReceive(message);
            Console.WriteLine("[SENSOR] Resposta: " + response);
        }
        catch (Exception ex)
        {
            Console.WriteLine("[SENSOR] Erro ao terminar comunicação: " + ex.Message);
        }
        finally
        {
            running = false;
        }
    }

    static void SendAutomaticHeartbeat()
    {
        while (running)
        {
            try
            {
                Thread.Sleep(15000);

                if (!running)
                    break;

                lock (commLock)
                {
                    if (!running)
                        break;

                    if (transferringVideo)
                        continue;

                    if (writer == null || reader == null)
                        break;

                    string timestamp = DateTime.Now.ToString("s");
                    string message = $"HEARTBEAT|{timestamp}|{sensorId}";
                    writer.WriteLine(message);

                    string response = reader.ReadLine();
                    Console.WriteLine("[SENSOR] Heartbeat automático enviado: " + response);
                }
            }
            catch
            {
                running = false;
                break;
            }
        }
    }
}