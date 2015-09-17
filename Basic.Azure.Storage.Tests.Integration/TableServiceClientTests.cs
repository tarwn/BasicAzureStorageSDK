using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Basic.Azure.Storage.Communications.TableService;
using Microsoft.WindowsAzure.Storage;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Tests.Integration
{
    [TestFixture]
    public class TableServiceClientTests
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

        #region Account Operations

        #endregion

        #region Table Operations

        [Test]
        public void CreateTable_RequiredArgsOnly_CreatesTable()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = GenerateSampleTableName();

            client.CreateTable(tableName);

            AssertTableExists(tableName);
        }

        [Test]
        [ExpectedException(typeof(TableAlreadyExistsAzureException))]
        public void CreateTable_TableAlreadyExists_ReportsConflict()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = GenerateSampleTableName();
            CreateTable(tableName);

            client.CreateTable(tableName);

            // expects exception
        }

        [Test]
        public void CreateTable_ValidName_ReceivesFullUrlInResponse()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = GenerateSampleTableName();

            var response = client.CreateTable(tableName);

            // I'm just looking for link text that has the name, not a direct match 
            //  against what they happen to be repsonding with right now
            string expectedLinkPattern = "https?://.*" + tableName + ".*";
            Assert.IsTrue(Regex.IsMatch(response.Link, expectedLinkPattern));
        }

        [Test]
        [Ignore("Not implemented by emulator or it chooses to ignore preferece every time (per API doc, missing header means it ignored the preference)")]
        public void CreateTable_SpecifyMetadataPreference_IndicatesIfPreferenceWasApplied()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = GenerateSampleTableName();

            var response = client.CreateTable(tableName, MetadataPreference.ReturnNoContent);

            Assert.AreEqual(MetadataPreference.ReturnNoContent, response.MetadataPreferenceApplied);
        }

        [Test]
        public async Task CreateTableAsync_RequiredArgsOnly_CreatesTable()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = GenerateSampleTableName();

            await client.CreateTableAsync(tableName);

            AssertTableExists(tableName);
        }

        [Test]
        [ExpectedException(typeof(TableAlreadyExistsAzureException))]
        public async Task CreateTableAsync_TableAlreadyExists_ReportsConflict()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = GenerateSampleTableName();
            CreateTable(tableName);

            await client.CreateTableAsync(tableName);

            // expects exception
        }

        #endregion

        #region Entity Operations

        #endregion

        #region Assertions

        private void AssertTableExists(string tableName)
        {
            var client = _storageAccount.CreateCloudTableClient();
            var table = client.GetTableReference(tableName);
            if(!table.Exists())
                Assert.Fail(String.Format("The table '{0}' does not exist", tableName));
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
