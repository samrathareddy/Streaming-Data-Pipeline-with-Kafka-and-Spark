namespace DataPipelineApi.Services;
public interface ICIService
{
  Task<string> TriggerWorkflowAsync(string workflowFile, string branch);
}
