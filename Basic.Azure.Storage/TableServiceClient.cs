using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.TableService;
using Basic.Azure.Storage.Communications.TableService.EntityOperations;
using Basic.Azure.Storage.Communications.TableService.Interfaces;
using Basic.Azure.Storage.Communications.TableService.TableOperations;
using Microsoft.Practices.TransientFaultHandling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage
{
    public class TableServiceClient : ITableServiceClient
    {
        private StorageAccountSettings _account;
        private RetryPolicy _optionalRetryPolicy;

        public TableServiceClient(StorageAccountSettings account, RetryPolicy optionalRetryPolicy = null)
        {
            _account = account;
            _optionalRetryPolicy = optionalRetryPolicy;
        }

        #region Account Operations

        #endregion

        #region Table Operations

        public QueryTablesResponse QueryTables()
        {
            var request = new QueryTablesRequest(_account);
            var response = request.Execute();
            return response.Payload;
        }
        public async Task<QueryTablesResponse> QueryTablesAsync()
        {
            var request = new QueryTablesRequest(_account);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }

        public CreateTableResponse CreateTable(string tableName, MetadataPreference? metadataPreference = null)
        {
            var request = new CreateTableRequest(_account, tableName, metadataPreference);
            var response = request.Execute(_optionalRetryPolicy);
            return response.Payload;
        }
        public async Task<CreateTableResponse> CreateTableAsync(string tableName, MetadataPreference? metadataPreference = null)
        {
            var request = new CreateTableRequest(_account, tableName, metadataPreference);
            var response = await request.ExecuteAsync(_optionalRetryPolicy);
            return response.Payload;
        }

        #endregion

        #region Entity Operations

        public QueryEntitiesResponse<TEntity> QueryEntities<TEntity>(string tableName, string partitionKey, string rowKey)
            where TEntity : ITableEntity, new()
        {
            var request = new QueryEntitiesRequest<TEntity>(_account, tableName, partitionKey, rowKey);
            var response = request.Execute(_optionalRetryPolicy);
            return response.Payload;
        }
        public async Task<QueryEntitiesResponse<TEntity>> QueryEntitiesAsync<TEntity>(string tableName, string partitionKey, string rowKey)
            where TEntity : ITableEntity, new()
        {
            var request = new QueryEntitiesRequest<TEntity>(_account, tableName, partitionKey, rowKey);
            var response = await request.ExecuteAsync(_optionalRetryPolicy);
            return response.Payload;
        }

        public InsertEntityResponse InsertEntity<TEntity>(string tableName, TEntity entity)
            where TEntity : ITableEntity, new()
        {
            var request = new InsertEntityRequest<TEntity>(_account, tableName, entity);
            var response = request.Execute(_optionalRetryPolicy);
            return response.Payload;
        }

        public async Task<InsertEntityResponse> InsertEntityAsync<TEntity>(string tableName, TEntity entity)
            where TEntity : ITableEntity, new()
        {
            var request = new InsertEntityRequest<TEntity>(_account, tableName, entity);
            var result = await request.ExecuteAsync(_optionalRetryPolicy);
            return result.Payload;
        }

        public UpdateEntityResponse UpdateEntity<TEntity>(string tableName, TEntity entity, string etag = null)
            where TEntity : ITableEntity, new()
        {
            var request = new UpdateEntityRequest<TEntity>(_account, tableName, entity, etag);
            var response = request.Execute(_optionalRetryPolicy);
            return response.Payload;
        }

        public async Task<UpdateEntityResponse> UpdateEntityAsync<TEntity>(string tableName, TEntity entity, string etag = null)
            where TEntity : ITableEntity, new()
        {
            var request = new UpdateEntityRequest<TEntity>(_account, tableName, entity, etag);
            var result = await request.ExecuteAsync(_optionalRetryPolicy);
            return result.Payload;
        }

        public MergeEntityResponse MergeEntity<TEntity>(string tableName, TEntity entity, string etag = null)
            where TEntity : ITableEntity, new()
        {
            var request = new MergeEntityRequest<TEntity>(_account, tableName, entity, etag);
            var response = request.Execute(_optionalRetryPolicy);
            return response.Payload;
        }

        public async Task<MergeEntityResponse> MergeEntityAsync<TEntity>(string tableName, TEntity entity, string etag = null)
            where TEntity : ITableEntity, new()
        {
            var request = new MergeEntityRequest<TEntity>(_account, tableName, entity, etag);
            var result = await request.ExecuteAsync(_optionalRetryPolicy);
            return result.Payload;
        }

        public void DeleteEntity(string tableName, string partitionKey, string rowKey, string etag = null)
        {
            var request = new DeleteEntityRequest(_account, tableName, partitionKey, rowKey, etag);
            request.Execute(_optionalRetryPolicy);
        }

        public async Task DeleteEntityAsync(string tableName, string partitionKey, string rowKey, string etag = null)
        {
            var request = new DeleteEntityRequest(_account, tableName, partitionKey, rowKey, etag);
            await request.ExecuteAsync(_optionalRetryPolicy);
        }

        public InsertOrReplaceEntityResponse InsertOrReplaceEntity<TEntity>(string tableName, TEntity entity)
            where TEntity : ITableEntity, new()
        {
            var request = new InsertOrReplaceEntityRequest<TEntity>(_account, tableName, entity);
            var response = request.Execute(_optionalRetryPolicy);
            return response.Payload;
        }

        public async Task<InsertOrReplaceEntityResponse> InsertOrReplaceEntityAsync<TEntity>(string tableName, TEntity entity)
            where TEntity : ITableEntity, new()
        {
            var request = new InsertOrReplaceEntityRequest<TEntity>(_account, tableName, entity);
            var result = await request.ExecuteAsync(_optionalRetryPolicy);
            return result.Payload;
        }

        public InsertOrMergeEntityResponse InsertOrMergeEntity<TEntity>(string tableName, TEntity entity)
            where TEntity : ITableEntity, new()
        {
            var request = new InsertOrMergeEntityRequest<TEntity>(_account, tableName, entity);
            var response = request.Execute(_optionalRetryPolicy);
            return response.Payload;
        }

        public async Task<InsertOrMergeEntityResponse> InsertOrMergeEntityAsync<TEntity>(string tableName, TEntity entity)
            where TEntity : ITableEntity, new()
        {
            var request = new InsertOrMergeEntityRequest<TEntity>(_account, tableName, entity);
            var result = await request.ExecuteAsync(_optionalRetryPolicy);
            return result.Payload;
        }

        #endregion



    }
}
