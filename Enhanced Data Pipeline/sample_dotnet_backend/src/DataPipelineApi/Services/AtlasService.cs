using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using DataPipelineApi.Options;

namespace DataPipelineApi.Services
{
    public class AtlasService : IAtlasService
    {
        private readonly HttpClient _http;

        public AtlasService(HttpClient http, IOptions<AtlasOptions> opt)
        {
            _http = http;
            _http.BaseAddress = new Uri(opt.Value.Endpoint);
            var token = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{opt.Value.Username}:{opt.Value.Password}"));
            _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", token);
        }

        public async Task<string> RegisterLineageAsync(string payload)
        {
            var content = new StringContent(payload, Encoding.UTF8, "application/json");
            var resp = await _http.PostAsync("/lineage", content);
            resp.EnsureSuccessStatusCode();
            return await resp.Content.ReadAsStringAsync();
        }
    }
}
