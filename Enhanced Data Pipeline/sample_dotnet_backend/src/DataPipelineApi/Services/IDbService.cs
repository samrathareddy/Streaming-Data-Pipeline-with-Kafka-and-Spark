using System.Collections.Generic;
namespace DataPipelineApi.Services;
public interface IDbService
{
  Task<IEnumerable<dynamic>> QueryMySqlAsync(string sql);
  Task ExecutePostgresAsync(string sql);
}
