using Basic.Azure.Storage.Communications.TableService;
using Basic.Azure.Storage.Communications.TableService.EntityOperations;
using Basic.Azure.Storage.Communications.TableService.Interfaces;
using Basic.Azure.Storage.Communications.TableService.TableOperations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.ClientContracts
{
    public interface ITableServiceClient
    {

        #region Account Operations

        #endregion

        #region Table Operations

        QueryTablesResponse QueryTables();
        Task<QueryTablesResponse> QueryTablesAsync();

        CreateTableResponse CreateTable(string tableName, MetadataPreference? metadataPreference = null);
        Task<CreateTableResponse> CreateTableAsync(string tableName, MetadataPreference? metadataPreference = null);

        #endregion

        #region Entity Operations

        InsertEntityResponse InsertEntity<TEntity>(string tableName, TEntity entity)
            where TEntity : ITableEntity, new();
        Task<InsertEntityResponse> InsertEntityAsync<TEntity>(string tableName, TEntity entity)
            where TEntity : ITableEntity, new();

        UpdateEntityResponse UpdateEntity<TEntity>(string tableName, TEntity sampleEntity, string etag = null)
            where TEntity : ITableEntity, new();
        Task<UpdateEntityResponse> UpdateEntityAsync<TEntity>(string tableName, TEntity sampleEntity, string etag = null)
            where TEntity : ITableEntity, new();

        MergeEntityResponse MergeEntity<TEntity>(string tableName, TEntity sampleEntity, string etag = null)
            where TEntity : ITableEntity, new();
        Task<MergeEntityResponse> MergeEntityAsync<TEntity>(string tableName, TEntity sampleEntity, string etag = null)
            where TEntity : ITableEntity, new();
        
        #endregion




    }
}
