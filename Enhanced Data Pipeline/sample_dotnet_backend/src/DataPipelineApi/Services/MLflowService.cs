using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;
using DataPipelineApi.Options;

namespace DataPipelineApi.Services
{
    public class MLflowService : IMLflowService
    {
        private readonly HttpClient _http;

        public MLflowService(HttpClient http, IOptions<MLflowOptions> opt)
        {
            _http = http;
            _http.BaseAddress = new Uri(opt.Value.TrackingUri);
        }

        public async Task<string> CreateRunAsync(string experimentId, string runName)
        {
            var obj = new JObject
            {
                ["experiment_id"] = experimentId,
                ["run_name"] = runName
            };
            var content = new StringContent(obj.ToString(), Encoding.UTF8, "application/json");
            var resp = await _http.PostAsync("/api/2.0/mlflow/runs/create", content);
            resp.EnsureSuccessStatusCode();
            return await resp.Content.ReadAsStringAsync();
        }
    }
}
