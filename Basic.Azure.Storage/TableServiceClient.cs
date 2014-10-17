using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.TableService;
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

        public TableServiceClient(StorageAccountSettings account)
        {
            _account = account;
        }

        #region Account Operations

        #endregion

        #region Table Operations

        public CreateTableResponse CreateTable(string tableName, MetadataPreference? metadataPreference = null)
        {
            var request = new CreateTableRequest(_account, tableName, metadataPreference);
            var response = request.Execute();
            return response.Payload;
        }
        public async Task<CreateTableResponse> CreateTableAsync(string tableName, MetadataPreference? metadataPreference = null)
        {
            var request = new CreateTableRequest(_account, tableName, metadataPreference);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }

        #endregion

        #region Entity Operations

        #endregion
    }
}
