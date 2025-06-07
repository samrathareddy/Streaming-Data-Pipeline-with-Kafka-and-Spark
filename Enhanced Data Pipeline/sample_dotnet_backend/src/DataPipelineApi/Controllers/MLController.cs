using Microsoft.AspNetCore.Mvc;
using DataPipelineApi.Services;

namespace DataPipelineApi.Controllers;
[ApiController]
[Route("api/ml")]
public class MLController : ControllerBase
{
  private readonly IMLflowService _ml;
  public MLController(IMLflowService ml) => _ml = ml;

  [HttpPost("run")]
  public async Task<IActionResult> Run([FromQuery]string expId, [FromQuery]string name)
  {
    var res = await _ml.CreateRunAsync(expId, name);
    return Ok(new { result = res });
  }
}
