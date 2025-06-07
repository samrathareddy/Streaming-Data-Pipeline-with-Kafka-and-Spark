namespace DataPipelineApi.Services;
public interface IMLflowService
{
  Task<string> CreateRunAsync(string experimentId, string runName);
}
