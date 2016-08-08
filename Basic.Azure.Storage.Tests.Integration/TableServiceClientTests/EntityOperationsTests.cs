using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Basic.Azure.Storage.Communications.TableService;
using Basic.Azure.Storage.Tests.Integration.Fakes;
using Microsoft.WindowsAzure.Storage;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Tests.Integration.TableServiceClientsTests
{
    [TestFixture]
    public class EntityOperationsTests
    {
        private StorageAccountSettings _accountSettings = StorageAccountSettings.Parse(ConfigurationManager.AppSettings["AzureConnectionString"]);
        private CloudStorageAccount _storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["AzureConnectionString"]);

        private List<string> _tablesToCleanup = new List<string>();

        private string GenerateSampleTableName()
        {
            var name = "unittest" + Guid.NewGuid().ToString("N").ToLower();
            _tablesToCleanup.Add(name);
            return name;
        }

        [TestFixtureTearDown]
        public void TestFixtureTeardown()
        {
            //let's clean up!
            var client = _storageAccount.CreateCloudTableClient();
            foreach (var tableName in _tablesToCleanup)
            {
                var table = client.GetTableReference(tableName);
                table.DeleteIfExists();
            }
        }

        #region Entity Operations

        #endregion
    }
}
