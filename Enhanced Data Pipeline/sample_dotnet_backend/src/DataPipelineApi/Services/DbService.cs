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
