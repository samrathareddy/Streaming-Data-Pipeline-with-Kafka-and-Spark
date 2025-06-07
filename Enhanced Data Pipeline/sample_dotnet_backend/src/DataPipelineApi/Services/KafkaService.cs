using Confluent.Kafka;
using Microsoft.Extensions.Options;
using DataPipelineApi.Options;

namespace DataPipelineApi.Services;
public class KafkaService : IKafkaService
{
  private readonly IProducer<Null,string> _p;
  private readonly string _topic;
  public KafkaService(IOptions<KafkaOptions> opt)
  {
    var v = opt.Value;
    _topic = v.Topic;
    _p = new ProducerBuilder<Null,string>(new ProducerConfig { BootstrapServers = v.BootstrapServers }).Build();
  }
  public async Task ProduceAsync(string message)
    => await _p.ProduceAsync(_topic, new Message<Null,string> { Value = message });
}
