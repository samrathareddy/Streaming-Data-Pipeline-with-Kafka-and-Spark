namespace DataPipelineApi.Services;
public interface IBatchService
{
  Task<string> TriggerBatchAsync();
  Task<string> GetBatchStatusAsync(string runId);
}
