#!/usr/bin/env bash
set -euo pipefail

BASE="sample_dotnet_backend"

# Remove old if exists
rm -rf "$BASE"

# 1. Directories
mkdir -p \
  "$BASE/src/DataPipelineApi/Controllers" \
  "$BASE/src/DataPipelineApi/Models" \
  "$BASE/src/DataPipelineApi/Options" \
  "$BASE/src/DataPipelineApi/Services"

# 2. appsettings.json
cat > "$BASE/appsettings.json" << 'EOF'
{
  "ConnectionStrings": {
    "MySql": "Server=mysql;Database=source_db;User=user;Password=pass;",
    "Postgres": "Host=postgres;Database=processed_db;Username=user;Password=pass;"
  },
  "Minio": {
    "Endpoint": "minio:9000",
    "AccessKey": "minio",
    "SecretKey": "minio123",
    "BucketRaw": "raw-data",
    "BucketProcessed": "processed-data"
  },
  "Kafka": {
    "BootstrapServers": "kafka:9092",
    "Topic": "events"
  },
  "Airflow": {
    "BaseUrl": "http://airflow:8080/api/v1",
    "Username": "airflow_user",
    "Password": "airflow_pass"
  },
  "GreatExpectations": {
    "CliPath": "/app/ge/bin/great_expectations"
  },
  "Atlas": {
    "Endpoint": "http://atlas:21000/api/atlas/v2",
    "Username": "admin",
    "Password": "admin"
  },
  "MLflow": {
    "TrackingUri": "http://mlflow:5000"
  },
  "GitHub": {
    "ActionsApi": "https://api.github.com/repos/hoangsonww/data-pipeline-api/actions/workflows",
    "Token": ""
  }
}
EOF

# 3. csproj
cat > "$BASE/src/DataPipelineApi/DataPipelineApi.csproj" << 'EOF'
<Project Sdk="Microsoft.NET.Sdk.Web">
  <PropertyGroup>
    <TargetFramework>net6.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Dapper" Version="2.0.123" />
    <PackageReference Include="MySqlConnector" Version="2.3.0" />
    <PackageReference Include="Npgsql" Version="7.0.4" />
    <PackageReference Include="AWSSDK.S3" Version="3.7.1.8" />
    <PackageReference Include="Confluent.Kafka" Version="2.2.0" />
    <PackageReference Include="Microsoft.Extensions.Http.Polly" Version="6.0.0" />
    <PackageReference Include="Swashbuckle.AspNetCore" Version="6.4.0" />
    <PackageReference Include="Microsoft.AspNetCore.Mvc.NewtonsoftJson" Version="6.0.0" />
    <PackageReference Include="Newtonsoft.Json" Version="13.0.1" />
    <PackageReference Include="Polly.Extensions.Http" Version="3.0.0" />
  </ItemGroup>
</Project>
EOF

# 4. Program.cs
cat > "$BASE/src/DataPipelineApi/Program.cs" << 'EOF'
using DataPipelineApi.Options;
using DataPipelineApi.Services;
using Microsoft.OpenApi.Models;
using Polly;
using Polly.Extensions.Http;

var builder = WebApplication.CreateBuilder(args);

// configure
builder.Services.Configure<DatabaseOptions>(builder.Configuration.GetSection("ConnectionStrings"));
builder.Services.Configure<MinioOptions>(builder.Configuration.GetSection("Minio"));
builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));
builder.Services.Configure<AirflowOptions>(builder.Configuration.GetSection("Airflow"));
builder.Services.Configure<GEOptions>(builder.Configuration.GetSection("GreatExpectations"));
builder.Services.Configure<AtlasOptions>(builder.Configuration.GetSection("Atlas"));
builder.Services.Configure<MLflowOptions>(builder.Configuration.GetSection("MLflow"));
builder.Services.Configure<GitHubOptions>(builder.Configuration.GetSection("GitHub"));

// http clients with retry
builder.Services.AddHttpClient<IBatchService, BatchService>()
  .AddPolicyHandler(HttpPolicyExtensions.HandleTransientHttpError()
    .WaitAndRetryAsync(3, retry => TimeSpan.FromSeconds(Math.Pow(2, retry))));
builder.Services.AddHttpClient<IStreamingService, StreamingService>()
  .AddPolicyHandler(HttpPolicyExtensions.HandleTransientHttpError()
    .WaitAndRetryAsync(3, retry => TimeSpan.FromSeconds(Math.Pow(2, retry))));
builder.Services.AddHttpClient<IAtlasService, AtlasService>();
builder.Services.AddHttpClient<IMLflowService, MLflowService>();
builder.Services.AddHttpClient<ICIService, CIService>();

// core services
builder.Services.AddSingleton<IDbService, DbService>();
builder.Services.AddSingleton<IStorageService, MinioService>();
builder.Services.AddSingleton<IKafkaService, KafkaService>();
builder.Services.AddSingleton<IGEValidationService, GEValidationService>();
builder.Services.AddSingleton<IMonitoringService, MonitoringService>();

builder.Services.AddControllers().AddNewtonsoftJson();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
  c.SwaggerDoc("v1", new OpenApiInfo { Title = "Full Data Pipeline API", Version = "v1" });
});

var app = builder.Build();
if (app.Environment.IsDevelopment())
{
  app.UseDeveloperExceptionPage();
  app.UseSwagger();
  app.UseSwaggerUI();
}
app.UseHttpsRedirection();
app.MapControllers();
app.Run();
EOF

# 5. Options
declare -a opts=(
  "DatabaseOptions|namespace DataPipelineApi.Options; public class DatabaseOptions { public string MySql { get; set; } = \"\"; public string Postgres { get; set; } = \"\"; }"
  "MinioOptions|namespace DataPipelineApi.Options; public class MinioOptions { public string Endpoint { get; set; } = \"\"; public string AccessKey { get; set; } = \"\"; public string SecretKey { get; set; } = \"\"; public string BucketRaw { get; set; } = \"\"; public string BucketProcessed { get; set; } = \"\"; }"
  "KafkaOptions|namespace DataPipelineApi.Options; public class KafkaOptions { public string BootstrapServers { get; set; } = \"\"; public string Topic { get; set; } = \"\"; }"
  "AirflowOptions|namespace DataPipelineApi.Options; public class AirflowOptions { public string BaseUrl { get; set; } = \"\"; public string Username { get; set; } = \"\"; public string Password { get; set; } = \"\"; }"
  "GEOptions|namespace DataPipelineApi.Options; public class GEOptions { public string CliPath { get; set; } = \"\"; }"
  "AtlasOptions|namespace DataPipelineApi.Options; public class AtlasOptions { public string Endpoint { get; set; } = \"\"; public string Username { get; set; } = \"\"; public string Password { get; set; } = \"\"; }"
  "MLflowOptions|namespace DataPipelineApi.Options; public class MLflowOptions { public string TrackingUri { get; set; } = \"\"; }"
  "GitHubOptions|namespace DataPipelineApi.Options; public class GitHubOptions { public string ActionsApi { get; set; } = \"\"; public string Token { get; set; } = \"\"; }"
)

for o in "${opts[@]}"; do
  IFS="|" read -r name body <<< "$o"
  cat > "$BASE/src/DataPipelineApi/Options/${name}.cs" << EOF
${body}
EOF
done

# 6. Models
cat > "$BASE/src/DataPipelineApi/Models/BatchRequest.cs" << 'EOF'
namespace DataPipelineApi.Models;
public class BatchRequest { public string SourceTable { get; set; } = ""; }
public class BatchResponse { public string RunId { get; set; } = ""; public string GEReport { get; set; } = ""; }
EOF

cat > "$BASE/src/DataPipelineApi/Models/StreamingRequest.cs" << 'EOF'
namespace DataPipelineApi.Models;
public class StreamingRequest { public int Partition { get; set; } = 0; }
public class StreamingResponse { public string RunId { get; set; } = ""; }
EOF

# 7. Services
# 7.1 DbService
cat > "$BASE/src/DataPipelineApi/Services/IDbService.cs" << 'EOF'
using System.Collections.Generic;
namespace DataPipelineApi.Services;
public interface IDbService
{
  Task<IEnumerable<dynamic>> QueryMySqlAsync(string sql);
  Task ExecutePostgresAsync(string sql);
}
EOF

cat > "$BASE/src/DataPipelineApi/Services/DbService.cs" << 'EOF'
using Dapper;
using MySqlConnector;
using Npgsql;
using Microsoft.Extensions.Options;
using DataPipelineApi.Options;

namespace DataPipelineApi.Services;
public class DbService : IDbService
{
  private readonly string _myCs, _pgCs;
  public DbService(IOptions<DatabaseOptions> opt)
  {
    _myCs = opt.Value.MySql;
    _pgCs = opt.Value.Postgres;
  }
  public async Task<IEnumerable<dynamic>> QueryMySqlAsync(string sql)
  {
    await using var conn = new MySqlConnection(_myCs);
    return await conn.QueryAsync(sql);
  }
  public async Task ExecutePostgresAsync(string sql)
  {
    await using var conn = new NpgsqlConnection(_pgCs);
    await conn.ExecuteAsync(sql);
  }
}
EOF

# 7.2 MinioService
cat > "$BASE/src/DataPipelineApi/Services/IStorageService.cs" << 'EOF'
using System.IO;
namespace DataPipelineApi.Services;
public interface IStorageService
{
  Task UploadRawAsync(string key, Stream data);
  Task<Stream> DownloadRawAsync(string key);
  Task UploadProcessedAsync(string key, Stream data);
}
EOF

cat > "$BASE/src/DataPipelineApi/Services/MinioService.cs" << 'EOF'
using System.IO;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Options;
using DataPipelineApi.Options;

namespace DataPipelineApi.Services;
public class MinioService : IStorageService
{
  private readonly AmazonS3Client _s3;
  private readonly string _bRaw, _bProc;
  public MinioService(IOptions<MinioOptions> opt)
  {
    var v = opt.Value;
    _s3 = new AmazonS3Client(v.AccessKey, v.SecretKey,
      new AmazonS3Config { ServiceURL = $"http://{v.Endpoint}", ForcePathStyle = true });
    _bRaw = v.BucketRaw; _bProc = v.BucketProcessed;
  }
  public async Task UploadRawAsync(string key, Stream data)
    => await _s3.PutObjectAsync(new PutObjectRequest { BucketName = _bRaw, Key = key, InputStream = data });
  public async Task<Stream> DownloadRawAsync(string key)
  {
    var r = await _s3.GetObjectAsync(_bRaw, key);
    var ms = new MemoryStream(); await r.ResponseStream.CopyToAsync(ms); ms.Position = 0;
    return ms;
  }
  public async Task UploadProcessedAsync(string key, Stream data)
    => await _s3.PutObjectAsync(new PutObjectRequest { BucketName = _bProc, Key = key, InputStream = data });
}
EOF

# 7.3 KafkaService
cat > "$BASE/src/DataPipelineApi/Services/IKafkaService.cs" << 'EOF'
namespace DataPipelineApi.Services;
public interface IKafkaService
{
  Task ProduceAsync(string message);
}
EOF

cat > "$BASE/src/DataPipelineApi/Services/KafkaService.cs" << 'EOF'
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using DataPipelineApi.Options;

namespace DataPipelineApi.Services;
public class KafkaService : IKafkaService
{
  private readonly IProducer<Null,string> _p;
  private readonly string _topic;
  public KafkaService(IOptions<KafkaOptions> opt)
  {
    var v = opt.Value;
    _topic = v.Topic;
    _p = new ProducerBuilder<Null,string>(new ProducerConfig { BootstrapServers = v.BootstrapServers }).Build();
  }
  public async Task ProduceAsync(string message)
    => await _p.ProduceAsync(_topic, new Message<Null,string> { Value = message });
}
EOF

# 7.4 GEValidationService
cat > "$BASE/src/DataPipelineApi/Services/IGEValidationService.cs" << 'EOF'
namespace DataPipelineApi.Services;
public interface IGEValidationService
{
  Task<string> ValidateAsync(string suite);
}
EOF

cat > "$BASE/src/DataPipelineApi/Services/GEValidationService.cs" << 'EOF'
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
EOF

# 7.5 BatchService
cat > "$BASE/src/DataPipelineApi/Services/IBatchService.cs" << 'EOF'
namespace DataPipelineApi.Services;
public interface IBatchService
{
  Task<string> TriggerBatchAsync();
  Task<string> GetBatchStatusAsync(string runId);
}
EOF

cat > "$BASE/src/DataPipelineApi/Services/BatchService.cs" << 'EOF'
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
EOF

# 7.6 StreamingService
cat > "$BASE/src/DataPipelineApi/Services/IStreamingService.cs" << 'EOF'
namespace DataPipelineApi.Services;
public interface IStreamingService
{
  Task<string> TriggerStreamingAsync();
  Task<string> GetStreamingStatusAsync(string runId);
}
EOF

cat > "$BASE/src/DataPipelineApi/Services/StreamingService.cs" << 'EOF'
using System.Net.Http.Headers;
using System.Text;
using Microsoft.Extensions.Options;
using DataPipelineApi.Options;

namespace DataPipelineApi.Services;
public class StreamingService : IStreamingService
{
  private readonly HttpClient _http;
  public StreamingService(HttpClient http, IOptions<AirflowOptions> opt)
  {
    _http = http;
    var v = opt.Value;
    _http.BaseAddress = new Uri(v.BaseUrl);
    var tok = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{v.Username}:{v.Password}"));
    _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", tok);
  }
  public async Task<string> TriggerStreamingAsync()
  {
    var id = $"stream_{DateTime.UtcNow:yyyyMMddHHmmss}";
    await _http.PostAsJsonAsync("/dags/streaming_monitoring_dag/dagRuns", new { dag_run_id = id });
    return id;
  }
  public async Task<string> GetStreamingStatusAsync(string runId)
  {
    var r = await _http.GetAsync($"/dags/streaming_monitoring_dag/dagRuns/{runId}");
    return await r.Content.ReadAsStringAsync();
  }
}
EOF

# 7.7 AtlasService
cat > "$BASE/src/DataPipelineApi/Services/IAtlasService.cs" << 'EOF'
namespace DataPipelineApi.Services;
public interface IAtlasService
{
  Task<string> RegisterLineageAsync(string payload);
}
EOF

cat > "$BASE/src/DataPipelineApi/Services/AtlasService.cs" << 'EOF'
using System.Net.Http.Headers;
using Microsoft.Extensions.Options;
using DataPipelineApi.Options;

namespace DataPipelineApi.Services;
public class AtlasService : IAtlasService
{
  private readonly HttpClient _http;
  public AtlasService(HttpClient http, IOptions<AtlasOptions> opt)
  {
    _http = http;
    _http.BaseAddress = new Uri(opt.Value.Endpoint);
    var tok = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{opt.Value.Username}:{opt.Value.Password}"));
    _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", tok);
  }
  public async Task<string> RegisterLineageAsync(string payload)
  {
    var r = await _http.PostAsync("lineage", new StringContent(payload, Encoding.UTF8, "application/json"));
    return await r.Content.ReadAsStringAsync();
  }
}
EOF

# 7.8 MLflowService
cat > "$BASE/src/DataPipelineApi/Services/IMLflowService.cs" << 'EOF'
namespace DataPipelineApi.Services;
public interface IMLflowService
{
  Task<string> CreateRunAsync(string experimentId, string runName);
}
EOF

cat > "$BASE/src/DataPipelineApi/Services/MLflowService.cs" << 'EOF'
using Microsoft.Extensions.Options;
using DataPipelineApi.Options;
using Newtonsoft.Json.Linq;

namespace DataPipelineApi.Services;
public class MLflowService : IMLflowService
{
  private readonly HttpClient _http;
  public MLflowService(HttpClient http, IOptions<MLflowOptions> opt)
  {
    _http = http;
    _http.BaseAddress = new Uri(opt.Value.TrackingUri);
  }
  public async Task<string> CreateRunAsync(string expId, string runName)
  {
    var obj = new JObject { ["experiment_id"] = expId, ["run_name"] = runName };
    var r = await _http.PostAsync("/api/2.0/mlflow/runs/create",
      new StringContent(obj.ToString(), Encoding.UTF8, "application/json"));
    return await r.Content.ReadAsStringAsync();
  }
}
EOF

# 7.9 CIService
cat > "$BASE/src/DataPipelineApi/Services/ICIService.cs" << 'EOF'
namespace DataPipelineApi.Services;
public interface ICIService
{
  Task<string> TriggerWorkflowAsync(string workflowFile, string branch);
}
EOF

cat > "$BASE/src/DataPipelineApi/Services/CIService.cs" << 'EOF'
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
EOF

# 7.10 MonitoringService
cat > "$BASE/src/DataPipelineApi/Services/IMonitoringService.cs" << 'EOF'
namespace DataPipelineApi.Services;
public interface IMonitoringService
{
  Task<string> GetHealthAsync();
}
EOF

cat > "$BASE/src/DataPipelineApi/Services/MonitoringService.cs" << 'EOF'
namespace DataPipelineApi.Services;
public class MonitoringService : IMonitoringService
{
  public Task<string> GetHealthAsync() => Task.FromResult("Healthy");
}
EOF

# 8. Controllers
# 8.1 BatchController
cat > "$BASE/src/DataPipelineApi/Controllers/BatchController.cs" << 'EOF'
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
EOF

# 8.2 StreamingController
cat > "$BASE/src/DataPipelineApi/Controllers/StreamingController.cs" << 'EOF'
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
EOF

# 8.3 GovernanceController
cat > "$BASE/src/DataPipelineApi/Controllers/GovernanceController.cs" << 'EOF'
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
EOF

# 8.4 MonitoringController
cat > "$BASE/src/DataPipelineApi/Controllers/MonitoringController.cs" << 'EOF'
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
EOF

# 8.5 MLController
cat > "$BASE/src/DataPipelineApi/Controllers/MLController.cs" << 'EOF'
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
EOF

# 8.6 CIController
cat > "$BASE/src/DataPipelineApi/Controllers/CIController.cs" << 'EOF'
using Microsoft.AspNetCore.Mvc;
using DataPipelineApi.Services;

namespace DataPipelineApi.Controllers;
[ApiController]
[Route("api/ci")]
public class CIController : ControllerBase
{
  private readonly ICIService _ci;
  public CIController(ICIService ci) => _ci = ci;

  [HttpPost("trigger")]
  public async Task<IActionResult> Trigger([FromQuery]string wf, [FromQuery]string branch)
  {
    var res = await _ci.TriggerWorkflowAsync(wf, branch);
    return Ok(new { result = res });
  }
}
EOF

# 9. Dockerfile
cat > "$BASE/Dockerfile" << 'EOF'
FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["src/DataPipelineApi/DataPipelineApi.csproj","DataPipelineApi/"]
RUN dotnet restore "DataPipelineApi/DataPipelineApi.csproj"
COPY src/DataPipelineApi/. ./DataPipelineApi
WORKDIR /src/DataPipelineApi
RUN dotnet publish -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
ENV ASPNETCORE_URLS=http://+:80
EXPOSE 80
ENTRYPOINT ["dotnet","DataPipelineApi.dll"]
EOF

echo "✅ Full comprehensive API scaffolded in '$BASE/'"
