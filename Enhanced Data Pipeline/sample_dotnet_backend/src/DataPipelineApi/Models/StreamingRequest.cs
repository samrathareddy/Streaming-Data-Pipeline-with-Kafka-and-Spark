namespace DataPipelineApi.Models;
public class StreamingRequest { public int Partition { get; set; } = 0; }
public class StreamingResponse { public string RunId { get; set; } = ""; }
