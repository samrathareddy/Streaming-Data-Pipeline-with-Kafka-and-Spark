using System.Net.Http.Headers;
using Microsoft.Extensions.Options;
using DataPipelineApi.Options;

namespace DataPipelineApi.Services;
public class CIService : ICIService
{
  private readonly HttpClient _http;
  private readonly string _api;
  public CIService(HttpClient http, IOptions<GitHubOptions> opt)
  {
    _http = http;
    _api = opt.Value.ActionsApi;
    _http.DefaultRequestHeaders.Authorization =
      new AuthenticationHeaderValue("Bearer", opt.Value.Token);
  }
  public async Task<string> TriggerWorkflowAsync(string wf, string branch)
  {
    var payload = new { @ref = branch };
    var r = await _http.PostAsJsonAsync($"{_api}/{wf}/dispatches", payload);
    return await r.Content.ReadAsStringAsync();
  }
}
