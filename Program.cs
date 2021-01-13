using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Public;
using Kafka.Public.Loggers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

/// https://www.youtube.com/watch?v=n_IQq3pze0s
/*
    docker pull confluentinc/cp-zookeeper                   //https://hub.docker.com/r/confluentinc/cp-zookeeper
    docker pull confluentinc/cp-kafka                       //https://hub.docker.com/r/confluentinc/cp-kafka

    // https://hub.docker.com/r/ches/kafka/
    docker network create kafka
    docker run -d --name=zookeeper --network=kafka -p 2181:2181 -e ZOOKEEPER_CLIENT_PORT=2181 -e ZOOKEEPER_TICK_TIME=2000 confluentinc/cp-zookeeper
    docker logs zookeeper                                   //Confirm that it's running
    docker run -d --name=kafka --network=kafka -p 9092:9092 -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 confluentinc/cp-kafka
    docker logs kafka                                       //Confirm that it's running

    Add 'Package References' to csproj file
        // https://www.nuget.org/packages/Confluent.Kafka/ - Confluent's .NET Client for Apache Kafka 
        <PackageReference Include="Confluent.Kafka" Version="1.5.3" />

        // https://www.nuget.org/packages/Microsoft.Extensions.Hosting/ - Hosting and startup infrastructures for applications.
        <PackageReference Include="Microsoft.Extensions.Hosting" Version="5.0.0" />

        // https://www.nuget.org/packages/kafka-sharp/ - High Performance .NET Kafka Client
        <PackageReference Include="kafka-sharp" Version="1.4.3" />

*/




namespace TestConnectToKafka
{
    class Program
    {
        static void Main(string[] args)
        {
            //Console.WriteLine("Hello World!");
            CreateHostBuilder(args).Build().Run();

        }

        private static IHostBuilder CreateHostBuilder(string[] args) => 
        Host.CreateDefaultBuilder(args)
        .ConfigureServices((context, collection) => 
        {
            collection.AddHostedService<KafkaConsumerHostedService>();
            collection.AddHostedService<KafkaProducerHostedService>();
        });
    }


    public class KafkaConsumerHostedService : IHostedService
    {
        private ILogger<KafkaConsumerHostedService> _logger;
        private ClusterClient _cluster;

        public KafkaConsumerHostedService(ILogger<KafkaConsumerHostedService> logger)
        {
            _logger = logger;
            _cluster = new ClusterClient(new Configuration
            {
                Seeds = "localhost:9092"
            }, new ConsoleLogger());
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _cluster.ConsumeFromLatest("demo");
            _cluster.MessageReceived += record => 
            {
                _logger.LogInformation($"The Received Value: {Encoding.UTF8.GetString(record.Value as byte[])}");
            };

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _cluster?.Dispose();
            return Task.CompletedTask;
        }
    }















    public class KafkaProducerHostedService : IHostedService
    {
        private ILogger<KafkaProducerHostedService> _logger;
        private IProducer<Null, string> _producer;

        public KafkaProducerHostedService(ILogger<KafkaProducerHostedService> logger)
        {
            _logger = logger;
            var config = new ProducerConfig(){
                BootstrapServers = "localhost:9092"
            };
            _producer = new ProducerBuilder<Null, string>(config).Build();
        } 
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            for(var i = 0;  i < 100; i++)
            {
                var value  = $"Hello World {i}";
                _logger.LogInformation($"The Produced Value: {value}");
                await _producer.ProduceAsync("demo", new Message<Null, string>()
                {
                    Value = value
                }, cancellationToken);

                _producer.Flush(TimeSpan.FromSeconds(10));
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _producer?.Dispose();
            return Task.CompletedTask;
        }
    }
}
