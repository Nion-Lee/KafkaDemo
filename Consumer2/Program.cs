using Confluent.Kafka;

namespace KafkaDemo
{
    class Program
    {
        public static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                GroupId = "test-consumer-group2",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                Console.WriteLine("Please choose a Topic to receive:");
                var topic = Console.ReadLine();
                consumer.Subscribe(topic);

                var tokenSource = new CancellationTokenSource();
                Console.CancelKeyPress += (_, eventArgs) =>
                {
                    eventArgs.Cancel = true;
                    tokenSource.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var result = consumer.Consume(tokenSource.Token);
                            Console.WriteLine($"Consumed message: {result.Message.Value} from topic {result.Topic} (partition:{result.Partition} / offset:{result.Offset})");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (Exception e)
                {
                    consumer.Close();
                }
            }
        }
    }
}
