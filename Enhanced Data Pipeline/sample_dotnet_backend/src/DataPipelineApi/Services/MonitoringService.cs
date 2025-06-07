namespace DataPipelineApi.Services;
public class MonitoringService : IMonitoringService
{
  public Task<string> GetHealthAsync() => Task.FromResult("Healthy");
}
