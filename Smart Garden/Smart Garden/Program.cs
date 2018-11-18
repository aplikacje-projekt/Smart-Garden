using System;
using System.Linq;
using System.Text;
using Dapper;
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Smart_Garden
{
    class Program
    {
        public static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "47.254.170.16",  UserName = "admin", Password = "admin", Port = 5672 };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "topic_logs", type: "topic");
                var queueName = channel.QueueDeclare().QueueName;

                if (args.Length < -1)
                {
                    Console.Error.WriteLine("Usage: {0} [binding_key...]",
                        Environment.GetCommandLineArgs()[0]);
                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                    Environment.ExitCode = 1;
                    return;
                }

                foreach (var bindingKey in args)
                {
                    channel.QueueBind(queue: queueName,
                        exchange: "amq.topic",
                        routingKey: bindingKey);
                }

                Console.WriteLine(" [*] Waiting for messages. To exit press CTRL+C");
                                                                 
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    var routingKey = ea.RoutingKey;
                    Console.WriteLine(" [x] Received '{0}':'{1}'",
                        routingKey,
                        message);
                    insertMeasurements(message);
                };
                channel.BasicConsume(queue: queueName,
                    autoAck: true,
                    consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }

        public static void insertMeasurements(string message)
        {
            using (var connection = new NpgsqlConnection("Host=47.254.170.16;Username=garden;Password=garden;Database=smart_garden"))
            {
                connection.Open();
                string temp = 12.12.ToString().Replace(',', '.');
                string air = 12.12.ToString().Replace(',', '.');
                string soil = 12.12.ToString().Replace(',', '.');
                string light = 12.12.ToString().Replace(',', '.');
                string water = 12.12.ToString().Replace(',', '.');
                string pump = 0.0.ToString().Replace(',', '.');
                string time = "2018-11-18 19:10:00";
                
                string insertString =
                    "INSERT INTO measurements (temperature, air_humidity, soil_humidity, light_density, water_level, pump_work, time) VALUES (" +
                    temp + ", " + air + ", " + soil + ", " + light + ", " + water + ", " + pump + ", " + "TIMESTAMP '" + time + "');";
                connection.Execute(insertString);
            }

        }
    }
}
