namespace DataPipelineApi.Services
{
    public interface IAtlasService
    {
        Task<string> RegisterLineageAsync(string payload);
    }
}
