using Confluent.Kafka;

namespace KafkaDemo
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            string? topic, message;
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
            };

            Console.WriteLine("Please choose a Topic to deliver:");
            topic = Console.ReadLine();

            while (true)
            {
                Console.WriteLine("Please enter the Message:");
                message = Console.ReadLine();

                try
                {
                    using (var producer = new ProducerBuilder<Null, string>(config).Build())
                    {
                        var result = await producer.ProduceAsync(topic, new Message<Null, string> { Value = message });
                        Console.WriteLine($"Delivered message: {result.Message.Value} to topic {result.Topic} (partition:{result.Partition} / offset:{result.Offset})");
                    }
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }

        public static void MainSecond(string[] args)
        {
            string? topic;
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
            };

            Console.WriteLine("Please choose a Topic to deliver:");
            topic = Console.ReadLine();

            Action<DeliveryReport<Null, string>> handler = report =>
                Console.WriteLine(!report.Error.IsError
                    ? $"Delivered message to {report.TopicPartitionOffset}"
                    : $"Delivery Error: {report.Error.Reason}");

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                for (int i = 0; i < 100; ++i)
                {
                    producer.Produce(topic, new Message<Null, string> { Value = i.ToString() }, handler);
                }
                producer.Flush(TimeSpan.FromSeconds(10));
            }
        }
    }
}
