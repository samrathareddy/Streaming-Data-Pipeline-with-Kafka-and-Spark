namespace DataPipelineApi.Services;
public interface IKafkaService
{
  Task ProduceAsync(string message);
}
