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

        /// <summary>
        /// Partial Implementation - No support for continuation yet
        /// </summary>
        QueryTablesResponse QueryTables();
        /// <summary>
        /// Partial Implementation - No support for continuation yet
        /// </summary>
        Task<QueryTablesResponse> QueryTablesAsync();

        CreateTableResponse CreateTable(string tableName, MetadataPreference? metadataPreference = null);
        Task<CreateTableResponse> CreateTableAsync(string tableName, MetadataPreference? metadataPreference = null);

        #endregion

        #region Entity Operations

        /// <summary>
        /// Partial implementation - currently only supports query for a specific Partition Key and Row Key, filter/select not
        /// yet supported
        /// </summary>
        QueryEntitiesResponse<TEntity> QueryEntities<TEntity>(string tableName, string partitionKey, string rowKey)
            where TEntity : ITableEntity, new();
        /// <summary>
        /// Partial implementation - currently only supports query for a specific Partition Key and Row Key, filter/select not
        /// yet supported
        /// </summary>
        Task<QueryEntitiesResponse<TEntity>> QueryEntitiesAsync<TEntity>(string tableName, string partitionKey, string rowKey)
            where TEntity : ITableEntity, new();


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

        void DeleteEntity(string tableName, string partitionKey, string rowKey, string etag = null);
        Task DeleteEntityAsync(string tableName, string partitionKey, string rowKey, string etag = null);

        InsertOrReplaceEntityResponse InsertOrReplaceEntity<TEntity>(string tableName, TEntity entity)
            where TEntity : ITableEntity, new();
        Task<InsertOrReplaceEntityResponse> InsertOrReplaceEntityAsync<TEntity>(string tableName, TEntity entity)
            where TEntity : ITableEntity, new();

        InsertOrMergeEntityResponse InsertOrMergeEntity<TEntity>(string tableName, TEntity entity)
            where TEntity : ITableEntity, new();
        Task<InsertOrMergeEntityResponse> InsertOrMergeEntityAsync<TEntity>(string tableName, TEntity entity)
            where TEntity : ITableEntity, new();

        #endregion








    }
}
