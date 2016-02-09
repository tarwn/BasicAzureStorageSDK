using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.TableService;
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

        #endregion
    }
}
