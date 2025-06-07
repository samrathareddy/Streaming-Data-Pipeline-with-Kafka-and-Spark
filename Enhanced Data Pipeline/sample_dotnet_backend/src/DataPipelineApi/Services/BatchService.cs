using System.Net.Http.Headers;
using System.Text;
using Microsoft.Extensions.Options;
using DataPipelineApi.Options;

namespace DataPipelineApi.Services;
public class BatchService : IBatchService
{
  private readonly HttpClient _http;
  public BatchService(HttpClient http, IOptions<AirflowOptions> opt)
  {
    _http = http;
    var v = opt.Value;
    _http.BaseAddress = new Uri(v.BaseUrl);
    var tok = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{v.Username}:{v.Password}"));
    _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", tok);
  }
  public async Task<string> TriggerBatchAsync()
  {
    var id = $"batch_{DateTime.UtcNow:yyyyMMddHHmmss}";
    await _http.PostAsJsonAsync("/dags/batch_ingestion_dag/dagRuns", new { dag_run_id = id });
    return id;
  }
  public async Task<string> GetBatchStatusAsync(string runId)
  {
    var r = await _http.GetAsync($"/dags/batch_ingestion_dag/dagRuns/{runId}");
    return await r.Content.ReadAsStringAsync();
  }
}
