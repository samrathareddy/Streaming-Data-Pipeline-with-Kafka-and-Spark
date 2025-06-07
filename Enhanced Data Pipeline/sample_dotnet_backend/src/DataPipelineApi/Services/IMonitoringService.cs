namespace DataPipelineApi.Services;
public interface IMonitoringService
{
  Task<string> GetHealthAsync();
}
