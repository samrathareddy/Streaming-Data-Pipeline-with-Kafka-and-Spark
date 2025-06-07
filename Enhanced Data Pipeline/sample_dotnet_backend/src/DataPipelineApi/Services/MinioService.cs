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
