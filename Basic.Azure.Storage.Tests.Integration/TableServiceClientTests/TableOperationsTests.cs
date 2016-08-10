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
    public class TableOperationsTests
    {
        private StorageAccountSettings _accountSettings = StorageAccountSettings.Parse(ConfigurationManager.AppSettings["AzureConnectionString"]);
        private TableUtil _util = new TableUtil(ConfigurationManager.AppSettings["AzureConnectionString"]);

        [TestFixtureTearDown]
        public void TestFixtureTeardown()
        {
            //let's clean up!
            _util.Cleanup();
        }

        #region Table Operations

        [Test]
        public void QueryTables_LessThan1000Tables_ReturnsList()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableList = new List<string>();
            for (int i = 0; i < 10; i++)
            {
                var name = _util.GenerateSampleTableName();
                tableList.Add(name);
                _util.CreateTable(name);
            }

            var response = client.QueryTables();

            int numExpectedTablesInResponse = 0;
            foreach (var expectedTable in tableList)
            {
                if (response.TableList.Contains(expectedTable))
                    numExpectedTablesInResponse++;
            }
            Assert.AreEqual(tableList.Count, numExpectedTablesInResponse);
        }

        [Test]
        public void CreateTable_RequiredArgsOnly_CreatesTable()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();

            client.CreateTable(tableName);

            _util.AssertTableExists(tableName);
        }

        [Test]
        [ExpectedException(typeof(TableAlreadyExistsAzureException))]
        public void CreateTable_TableAlreadyExists_ReportsConflict()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);

            client.CreateTable(tableName);

            // expects exception
        }

        [Test]
        public void CreateTable_ValidName_ReceivesFullUrlInResponse()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();

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
            var tableName = _util.GenerateSampleTableName();

            var response = client.CreateTable(tableName, MetadataPreference.ReturnNoContent);

            Assert.AreEqual(MetadataPreference.ReturnNoContent, response.MetadataPreferenceApplied);
        }

        [Test]
        public async Task CreateTableAsync_RequiredArgsOnly_CreatesTable()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();

            await client.CreateTableAsync(tableName);

            _util.AssertTableExists(tableName);
        }

        [Test]
        [ExpectedException(typeof(TableAlreadyExistsAzureException))]
        public async Task CreateTableAsync_TableAlreadyExists_ReportsConflict()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);

            await client.CreateTableAsync(tableName);

            // expects exception
        }

        #endregion

    }
}
