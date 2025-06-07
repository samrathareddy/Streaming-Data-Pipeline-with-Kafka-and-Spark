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
