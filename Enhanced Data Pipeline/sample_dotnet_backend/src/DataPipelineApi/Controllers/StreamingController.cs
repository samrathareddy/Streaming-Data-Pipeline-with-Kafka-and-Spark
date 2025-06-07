using DataPipelineApi.Models;
using DataPipelineApi.Services;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

namespace DataPipelineApi.Controllers;
[ApiController]
[Route("api/stream")]
public class StreamingController : ControllerBase
{
  private readonly IKafkaService _kaf;
  private readonly IStreamingService _airflow;

  public StreamingController(IKafkaService kaf, IStreamingService airflow)
  { _kaf=kaf; _airflow=airflow; }

  [HttpPost("produce")]
  public async Task<IActionResult> Produce([FromBody] StreamingRequest req)
  {
    var msg = JsonSerializer.Serialize(new { ts=DateTime.UtcNow, partition=req.Partition });
    await _kaf.ProduceAsync(msg);
    return Ok(new { status="sent" });
  }

  [HttpPost("run")]
  public async Task<StreamingResponse> Run()
    => new() { RunId = await _airflow.TriggerStreamingAsync() };
}
