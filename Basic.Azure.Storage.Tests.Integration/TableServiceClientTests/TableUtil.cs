using Basic.Azure.Storage.Tests.Integration.Fakes;
using Microsoft.WindowsAzure.Storage;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Tests.Integration.TableServiceClientTests
{
    public class TableUtil
    {
        public CloudStorageAccount _storageAccount;
        private List<string> _tablesToCleanup = new List<string>();

        public TableUtil(string connectionString)
        {
            _storageAccount = CloudStorageAccount.Parse(connectionString);
        }

        public string GenerateSampleTableName()
        {
            var name = "unittest" + Guid.NewGuid().ToString("N").ToLower();
            _tablesToCleanup.Add(name);
            return name;
        }

        public void Cleanup()
        {
            var client = _storageAccount.CreateCloudTableClient();
            foreach (var tableName in _tablesToCleanup)
            {
                var table = client.GetTableReference(tableName);
                table.DeleteIfExists();
            }
        }

        #region Assertions

        public void AssertTableExists(string tableName)
        {
            var client = _storageAccount.CreateCloudTableClient();
            var table = client.GetTableReference(tableName);
            if (!table.Exists())
                Assert.Fail(String.Format("The table '{0}' does not exist", tableName));
        }


        public void AssertEntityExists(string tableName, SampleEntity sampleEntity)
        {
            var client = _storageAccount.CreateCloudTableClient();
            var table = client.GetTableReference(tableName);
            if (!table.Exists())
                Assert.Fail(String.Format("The table '{0}' does not exist", tableName));
            var retrieveOperation = Microsoft.WindowsAzure.Storage.Table.TableOperation.Retrieve(sampleEntity.PartitionKey, sampleEntity.RowKey);
            var result = table.Execute(retrieveOperation);
            if (result.Result == null)
                Assert.Fail("The entity was not found in the table");
        }

        #endregion

        #region Setup Mechanics

        public void CreateTable(string tableName)
        {
            var client = _storageAccount.CreateCloudTableClient();
            var table = client.GetTableReference(tableName);
            table.Create();
        }

        #endregion
    }
}
