using System.Diagnostics;
using Microsoft.Extensions.Options;
using DataPipelineApi.Options;

namespace DataPipelineApi.Services;
public class GEValidationService : IGEValidationService
{
  private readonly string _cli;
  public GEValidationService(IOptions<GEOptions> opt) => _cli = opt.Value.CliPath;
  public Task<string> ValidateAsync(string suite)
  {
    var p = Process.Start(new ProcessStartInfo(_cli, $"checkpoint run {suite}") { RedirectStandardOutput = true })!;
    var outp = p.StandardOutput.ReadToEnd();
    p.WaitForExit();
    return Task.FromResult(outp);
  }
}
