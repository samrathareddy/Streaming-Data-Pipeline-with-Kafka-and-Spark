using DataPipelineApi.Models;
using DataPipelineApi.Services;
using Microsoft.AspNetCore.Mvc;
using System.IO;
using System.Text.Json;

namespace DataPipelineApi.Controllers;
[ApiController]
[Route("api/batch")]
public class BatchController : ControllerBase
{
  private readonly IDbService _db;
  private readonly IStorageService _st;
  private readonly IGEValidationService _ge;
  private readonly IBatchService _airflow;

  public BatchController(IDbService db, IStorageService st, IGEValidationService ge, IBatchService airflow)
  { _db=db; _st=st; _ge=ge; _airflow=airflow; }

  [HttpPost("ingest")]
  public async Task<BatchResponse> Ingest([FromBody] BatchRequest req)
  {
    var rows = await _db.QueryMySqlAsync($"SELECT * FROM {req.SourceTable}");
    await using var ms = new MemoryStream();
    await JsonSerializer.SerializeAsync(ms, rows);
    ms.Position = 0;
    await _st.UploadRawAsync($"{req.SourceTable}/{DateTime.UtcNow:yyyyMMddHHmmss}.json", ms);
    var report = await _ge.ValidateAsync("great_expectations/expectations");
    var run = await _airflow.TriggerBatchAsync();
    return new() { RunId = run, GEReport = report };
  }
}
