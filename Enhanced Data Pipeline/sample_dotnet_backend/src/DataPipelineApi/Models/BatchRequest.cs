namespace DataPipelineApi.Models;
public class BatchRequest { public string SourceTable { get; set; } = ""; }
public class BatchResponse { public string RunId { get; set; } = ""; public string GEReport { get; set; } = ""; }
