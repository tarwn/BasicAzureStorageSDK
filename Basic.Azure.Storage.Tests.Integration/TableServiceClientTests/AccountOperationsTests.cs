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

namespace Basic.Azure.Storage.Tests.Integration.TableServiceClientTests
{
    [TestFixture]
    public class AccountOperationsTests
    {
        private StorageAccountSettings _accountSettings = StorageAccountSettings.Parse(ConfigurationManager.AppSettings["AzureConnectionString"]);
        private TableUtil _util = new TableUtil(ConfigurationManager.AppSettings["AzureConnectionString"]);

        [TestFixtureTearDown]
        public void TestFixtureTeardown()
        {
            //let's clean up!
            _util.Cleanup();
        }

        #region Account Operations

        public void InsertEntity_ValidTable_InsertsEntityInTable()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);
            var sampleEntity = new SampleEntity();

            client.InsertEntity(tableName, sampleEntity);

            _util.AssertEntityExists(tableName, sampleEntity);
        }


        #endregion

    }
}
