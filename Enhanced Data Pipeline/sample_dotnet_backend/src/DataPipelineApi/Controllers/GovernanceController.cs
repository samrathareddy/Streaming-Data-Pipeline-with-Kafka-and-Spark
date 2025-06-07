using Microsoft.AspNetCore.Mvc;
using DataPipelineApi.Services;

namespace DataPipelineApi.Controllers;
[ApiController]
[Route("api/governance")]
public class GovernanceController : ControllerBase
{
  private readonly IAtlasService _atlas;
  public GovernanceController(IAtlasService atlas) => _atlas = atlas;

  [HttpPost("lineage")]
  public async Task<IActionResult> Lineage([FromBody] object payload)
  {
    var json = payload.ToString()!;
    var res = await _atlas.RegisterLineageAsync(json);
    return Ok(new { result = res });
  }
}
