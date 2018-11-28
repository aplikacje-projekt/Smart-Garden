using System;
using Newtonsoft.Json;
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
                try
                {
                    var deserializedMessage = JsonConvert.DeserializeObject<Message>(message);
                    int booleanConversion;
                    if (deserializedMessage.pump == "false")
                        booleanConversion = 0;
                    else
                        booleanConversion = 1;
                    string insertString =
                        "INSERT INTO measurements (dev, temp, air, soil, rain, water, light, pump, time) VALUES (" + "'" + deserializedMessage.dev + "'" + ", " +
                        deserializedMessage.temp + ", " + deserializedMessage.air + ", " + deserializedMessage.soil +
                        ", " + deserializedMessage.rain + ", " + deserializedMessage.water + ", " + deserializedMessage.light + ", " +
                        booleanConversion + ", " + "'" + DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss") + "');";
                    connection.Execute(insertString);
                    Console.WriteLine(insertString);
                }
                catch (Exception e)
                {
                    ;
                }
            }

        }
    }

    public class Message
    {
        public string dev { get; set; }
        public string temp { get; set; }
        public string air { get; set; }
        public string soil { get; set; }
        public string rain { get; set; }
        public string water { get; set; }
        public string light { get; set; }
        public string pump { get; set; }
    }
}
