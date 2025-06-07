using System.IO;
namespace DataPipelineApi.Services;
public interface IStorageService
{
  Task UploadRawAsync(string key, Stream data);
  Task<Stream> DownloadRawAsync(string key);
  Task UploadProcessedAsync(string key, Stream data);
}
