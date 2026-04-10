using System;
using System.IO;
using System.Net.Sockets;

class Program
{
    static string sensorId = "S102";
    static string zona = "ZONA_ESCOLAR";

    static TcpClient client;
    static StreamReader reader;
    static StreamWriter writer;

    static void Main(string[] args)
    {
        string gatewayIp = "127.0.0.1";
        int gatewayPort = 5000;

        try
        {
            client = new TcpClient();
            client.Connect(gatewayIp, gatewayPort);

            NetworkStream ns = client.GetStream();
            reader = new StreamReader(ns);
            writer = new StreamWriter(ns);
            writer.AutoFlush = true;

            SendHello();

            bool running = true;

            while (running)
            {
                Console.WriteLine();
                Console.WriteLine("=== MENU SENSOR ===");
                Console.WriteLine("1 - Enviar medição");
                Console.WriteLine("2 - Terminar");
                Console.Write("Escolha: ");

                string option = Console.ReadLine();

                switch (option)
                {
                    case "1":
                        SendMeasurement();
                        break;

                    case "2":
                        SendBye();
                        running = false;
                        break;

                    default:
                        Console.WriteLine("Opção inválida.");
                        break;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("Erro: " + ex.Message);
        }
        finally
        {
            if (reader != null) reader.Close();
            if (writer != null) writer.Close();
            if (client != null) client.Close();
        }
    }

    static void SendHello()
    {
        string message = "HELLO|" + sensorId + "|" + zona + "|TEMP";
        writer.WriteLine(message);

        string response = reader.ReadLine();
        Console.WriteLine("Resposta ao HELLO: " + response);
    }

    static void SendMeasurement()
    {
        Console.Write("Valor da temperatura: ");
        string valor = Console.ReadLine();

        if (string.IsNullOrWhiteSpace(valor))
        {
            Console.WriteLine("Valor inválido.");
            return;
        }

        string timestamp = DateTime.Now.ToString("s");
        string message = "DATA|" + timestamp + "|" + sensorId + "|" + zona + "|TEMP|" + valor;

        writer.WriteLine(message);

        string response = reader.ReadLine();
        Console.WriteLine("Resposta: " + response);
    }

    static void SendBye()
    {
        string message = "BYE|" + sensorId;
        writer.WriteLine(message);

        string response = reader.ReadLine();
        Console.WriteLine("Resposta: " + response);
    }
}