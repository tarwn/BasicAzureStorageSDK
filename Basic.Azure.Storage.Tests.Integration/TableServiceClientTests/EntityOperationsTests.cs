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
    public class EntityOperationsTests
    {
        private StorageAccountSettings _accountSettings = StorageAccountSettings.Parse(ConfigurationManager.AppSettings["AzureConnectionString"]);
        private TableUtil _util = new TableUtil(ConfigurationManager.AppSettings["AzureConnectionString"]);

        [TestFixtureTearDown]
        public void TestFixtureTeardown()
        {
            //let's clean up!
            _util.Cleanup();
        }

        #region Entity Operations

        [Test]
        public void InsertEntity_ValidTable_InsertsEntityInTable()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);
            var sampleEntity = new SampleEntity() { 
                PartitionKey = "1",
                RowKey = "A"
            };

            client.InsertEntity(tableName, sampleEntity);

            _util.AssertEntityExists(tableName, sampleEntity);
        }
        [Test]
        public async Task InsertEntityAsync_ValidTable_InsertsEntityInTable()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A"
            };

            await client.InsertEntityAsync(tableName, sampleEntity);

            _util.AssertEntityExists(tableName, sampleEntity);
        }

        [Test]
        [ExpectedException(typeof(TableNotFoundAzureException))]
        public void InsertEntity_InvalidTable_ExpectsException()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A"
            };

            client.InsertEntity(tableName, sampleEntity);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(EntityAlreadyExistsAzureException))]
        public void InsertEntity_PreexistingEntity_ExpectsException()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A"
            };
            _util.InsertTableEntity(tableName, sampleEntity.PartitionKey, sampleEntity.RowKey);

            client.InsertEntity(tableName, sampleEntity);

            // expects exception
        }

        [Test]
        public void UpdateEntity_ExistingEntity_UpdatesEntityInTable()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A",
                ExtraValue = "Extra"
            };
            _util.InsertTableEntity(tableName, sampleEntity.PartitionKey, sampleEntity.RowKey);

            client.UpdateEntity(tableName, sampleEntity);

            _util.AssertEntityExists(tableName, sampleEntity);
            var entity = _util.GetEntity<SampleMSEntity>(tableName, sampleEntity.PartitionKey, sampleEntity.RowKey);
            Assert.AreEqual(sampleEntity.ExtraValue, entity.ExtraValue);
        }
        [Test]
        public async Task UpdateEntityAsync_ExistingEntity_UpdatesEntityInTable()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A",
                ExtraValue = "Extra"
            };
            _util.InsertTableEntity(tableName, sampleEntity.PartitionKey, sampleEntity.RowKey);

            await client.UpdateEntityAsync(tableName, sampleEntity);

            _util.AssertEntityExists(tableName, sampleEntity);
            var entity = _util.GetEntity<SampleMSEntity>(tableName, sampleEntity.PartitionKey, sampleEntity.RowKey);
            Assert.AreEqual(sampleEntity.ExtraValue, entity.ExtraValue);
        }

        [Test]
        public void UpdateEntity_ExistingEntityWithMatchingRequiredETag_UpdatesEntityInTable()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A",
                ExtraValue = "Extra"
            };
            var etag = _util.InsertTableEntity(tableName, sampleEntity.PartitionKey, sampleEntity.RowKey);

            client.UpdateEntity(tableName, sampleEntity, etag);

            _util.AssertEntityExists(tableName, sampleEntity);
            var entity = _util.GetEntity<SampleMSEntity>(tableName, sampleEntity.PartitionKey, sampleEntity.RowKey);
            Assert.AreEqual(sampleEntity.ExtraValue, entity.ExtraValue);
        }

        [Test]
        [ExpectedException(typeof(UpdateConditionNotSatisfiedAzureException))]
        public void UpdateEntity_ExistingEntityWithMismatchedRequiredETag_ThrowsException()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A",
                ExtraValue = "Extra"
            };
            var etag = _util.InsertTableEntity(tableName, sampleEntity.PartitionKey, sampleEntity.RowKey);

            client.UpdateEntity(tableName, sampleEntity, etag.Replace("201","XXX"));    // etag includes a date string, so we can easily swap out part to create an invalid one

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(TableNotFoundAzureException))]
        public void UpdateEntity_InvalidTable_ExpectsException()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A",
                ExtraValue = "Extra"
            };

            client.UpdateEntity(tableName, sampleEntity);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(TableNotFoundAzureException))]
        public void UpdateEntity_NonexistentEntity_ExpectsException()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A",
                ExtraValue = "Extra"
            };

            client.UpdateEntity(tableName, sampleEntity);

            // expects exception
        }

        [Test]
        public void MergeEntity_ExistingEntity_MergesEntityInTable()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A",
                ExtraValue = "Extra"
            };
            _util.InsertTableEntity(tableName, sampleEntity.PartitionKey, sampleEntity.RowKey);

            client.MergeEntity(tableName, sampleEntity);

            _util.AssertEntityExists(tableName, sampleEntity);
            var entity = _util.GetEntity<SampleMSEntity>(tableName, sampleEntity.PartitionKey, sampleEntity.RowKey);
            Assert.AreEqual(sampleEntity.ExtraValue, entity.ExtraValue);
        }
        [Test]
        public async Task MergeEntityAsync_ExistingEntity_MergesEntityInTable()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A",
                ExtraValue = "Extra"
            };
            _util.InsertTableEntity(tableName, sampleEntity.PartitionKey, sampleEntity.RowKey);

            await client.MergeEntityAsync(tableName, sampleEntity);

            _util.AssertEntityExists(tableName, sampleEntity);
            var entity = _util.GetEntity<SampleMSEntity>(tableName, sampleEntity.PartitionKey, sampleEntity.RowKey);
            Assert.AreEqual(sampleEntity.ExtraValue, entity.ExtraValue);
        }

        [Test]
        public void MergeEntity_ExistingEntityWithMatchingRequiredETag_MergesEntityInTable()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A",
                ExtraValue = "Extra"
            };
            var etag = _util.InsertTableEntity(tableName, sampleEntity.PartitionKey, sampleEntity.RowKey);

            client.MergeEntity(tableName, sampleEntity, etag);

            _util.AssertEntityExists(tableName, sampleEntity);
            var entity = _util.GetEntity<SampleMSEntity>(tableName, sampleEntity.PartitionKey, sampleEntity.RowKey);
            Assert.AreEqual(sampleEntity.ExtraValue, entity.ExtraValue);
        }

        [Test]
        [ExpectedException(typeof(UpdateConditionNotSatisfiedAzureException))]
        public void MergeEntity_ExistingEntityWithMismatchedRequiredETag_ThrowsException()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A",
                ExtraValue = "Extra"
            };
            var etag = _util.InsertTableEntity(tableName, sampleEntity.PartitionKey, sampleEntity.RowKey);

            client.MergeEntity(tableName, sampleEntity, etag.Replace("201", "XXX"));    // etag includes a date string, so we can easily swap out part to create an invalid one

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(TableNotFoundAzureException))]
        public void MergeEntity_InvalidTable_ExpectsException()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A",
                ExtraValue = "Extra"
            };

            client.MergeEntity(tableName, sampleEntity);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(TableNotFoundAzureException))]
        public void MergeEntity_NonexistentEntity_ExpectsException()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A",
                ExtraValue = "Extra"
            };

            client.MergeEntity(tableName, sampleEntity);

            // expects exception
        }
        #endregion
    }
}
