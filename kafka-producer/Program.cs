using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace kafka_producer
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using (var produce = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var count = 0;
                    while (true)
                    {
                        var dr = await produce.ProduceAsync("test-topic", new Message<Null, string> { Value = $"testando kafka: {count++}" });
                        Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}' | {count}");

                        Thread.Sleep(2000);

                    }
                }
                catch (ProduceException<Null, string> ex)
                {
                    Console.WriteLine($"Delivered failed '{ex.Error.Reason}'");
                }
            }
        }
    }
}