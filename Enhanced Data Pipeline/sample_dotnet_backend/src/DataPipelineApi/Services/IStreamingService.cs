namespace DataPipelineApi.Services;
public interface IStreamingService
{
  Task<string> TriggerStreamingAsync();
  Task<string> GetStreamingStatusAsync(string runId);
}
