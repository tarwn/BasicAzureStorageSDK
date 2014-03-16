using Basic.Azure.Storage.Communications.TableService;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage
{
    public class TableServiceClient
    {
        private StorageAccountSettings _account;

        public TableServiceClient(StorageAccountSettings account)
        {
            _account = account;
        }

        public CreateTableResponse CreateTable(string tableName, MetadataPreference? metadataPreference = null)
        {
            var request = new CreateTableRequest(_account, tableName, metadataPreference);
            var response = request.Execute();
            return response.Payload;
        }
    }
}
