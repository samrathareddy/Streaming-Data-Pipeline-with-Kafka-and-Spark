namespace DataPipelineApi.Services;
public interface IGEValidationService
{
  Task<string> ValidateAsync(string suite);
}
