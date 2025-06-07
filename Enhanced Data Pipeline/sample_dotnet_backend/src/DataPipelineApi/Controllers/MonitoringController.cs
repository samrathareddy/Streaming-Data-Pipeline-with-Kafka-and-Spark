using Microsoft.AspNetCore.Mvc;
using DataPipelineApi.Services;

namespace DataPipelineApi.Controllers;
[ApiController]
[Route("api/monitor")]
public class MonitoringController : ControllerBase
{
  private readonly IMonitoringService _mon;
  public MonitoringController(IMonitoringService mon) => _mon = mon;

  [HttpGet("health")]
  public async Task<IActionResult> Health()
    => Ok(new { status = await _mon.GetHealthAsync() });
}
