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

        [Test]
        public void QueryEntities_PartitionAndRowKey_ReturnsSingleEntity()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);
            string partitionKey = "A";
            string rowKey = "1";
            _util.InsertTableEntity(tableName, partitionKey, rowKey);

            var response = client.QueryEntities<SampleEntity>(tableName, partitionKey, rowKey);
            
            Assert.AreEqual(1, response.Entities.Count);
            var entity = response.Entities[0];
            Assert.AreEqual(partitionKey, entity.PartitionKey);
            Assert.AreEqual(rowKey, entity.RowKey);
        }
        [Test]
        public async Task QueryEntitiesAsync_PartitionAndRowKey_ReturnsSingleEntity()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);
            string partitionKey = "A";
            string rowKey = "1";
            _util.InsertTableEntity(tableName, partitionKey, rowKey);

            var response = await client.QueryEntitiesAsync<SampleEntity>(tableName, partitionKey, rowKey);

            Assert.AreEqual(1, response.Entities.Count);
            var entity = response.Entities[0];
            Assert.AreEqual(partitionKey, entity.PartitionKey);
            Assert.AreEqual(rowKey, entity.RowKey);
        }

        [Test]
        [ExpectedException(typeof(ResourceNotFoundAzureException))]
        public void QueryEntities_NonExistentPartitionAndRowKey_ThrowsNotFound()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);
            string partitionKey = "A";
            string rowKey = "1";

            var response = client.QueryEntities<SampleEntity>(tableName, partitionKey, rowKey);

            //expects exception rather than empty list
        }


        [Test]
        [ExpectedException(typeof(TableNotFoundAzureException))]
        public void QueryEntities_NonExistentTable_ThrowsTableNotFound()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            string partitionKey = "A";
            string rowKey = "1";

            var response = client.QueryEntities<SampleEntity>(tableName, partitionKey, rowKey);

            //expects exception
        }
        
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


        [Test]
        public void DeleteEntity_ValidEntity_DeletesEntity()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            string partitionKey = "A";
            string rowKey = "1";
            _util.CreateTable(tableName);
            _util.InsertTableEntity(tableName, partitionKey, rowKey);

            client.DeleteEntity(tableName, partitionKey, rowKey);

            _util.AssertEntityDoesNotExist(tableName, partitionKey, rowKey);
        }


        [Test]
        public void DeleteEntity_ValidEntityWithRightETag_DeletesEntity()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            string partitionKey = "A";
            string rowKey = "1";
            _util.CreateTable(tableName);
            var etag = _util.InsertTableEntity(tableName, partitionKey, rowKey);

            client.DeleteEntity(tableName, partitionKey, rowKey, etag);

            _util.AssertEntityDoesNotExist(tableName, partitionKey, rowKey);
        }


        [Test]
        [ExpectedException(typeof(UpdateConditionNotSatisfiedAzureException))]
        public void DeleteEntity_ValidEntityWithWrongETag_ThrowsException()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            string partitionKey = "A";
            string rowKey = "1";
            _util.CreateTable(tableName);
            var etag = _util.InsertTableEntity(tableName, partitionKey, rowKey);

            client.DeleteEntity(tableName, partitionKey, rowKey, etag.Replace("201","XXX"));    // ms uses a date in the etag so we can replace part of it to invalidate it

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(ResourceNotFoundAzureException))]
        public void DeleteEntity_NonExistentEntity_ThrowsException()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            string partitionKey = "A";
            string rowKey = "1";
            _util.CreateTable(tableName);

            client.DeleteEntity(tableName, partitionKey, rowKey);

            // expects exception
        }
        
        [Test]
        [ExpectedException(typeof(TableNotFoundAzureException))]
        public void DeleteEntity_NonExistentTable_ThrowsException()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            string partitionKey = "A";
            string rowKey = "1";

            client.DeleteEntity(tableName, partitionKey, rowKey);

            // expects exception
        }

        [Test]
        public void InsertOrReplaceEntity_ValidTable_InsertsEntityInTable()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A"
            };

            client.InsertOrReplaceEntity(tableName, sampleEntity);

            _util.AssertEntityExists(tableName, sampleEntity);
        }
        [Test]
        public async Task InsertOrReplaceEntityAsync_ValidTable_InsertsEntityInTable()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A"
            };

            await client.InsertOrReplaceEntityAsync(tableName, sampleEntity);

            _util.AssertEntityExists(tableName, sampleEntity);
        }

        [Test]
        public void InsertOrReplaceEntity_ExistingEntity_UpdatesEntityInTable()
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

            client.InsertOrReplaceEntity(tableName, sampleEntity);

            _util.AssertEntityExists(tableName, sampleEntity);
            var entity = _util.GetEntity<SampleMSEntity>(tableName, sampleEntity.PartitionKey, sampleEntity.RowKey);
            Assert.AreEqual(sampleEntity.ExtraValue, entity.ExtraValue);
        }
        [Test]
        public async Task InsertOrReplaceEntityAsync_ExistingEntity_UpdatesEntityInTable()
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

            await client.InsertOrReplaceEntityAsync(tableName, sampleEntity);

            _util.AssertEntityExists(tableName, sampleEntity);
            var entity = _util.GetEntity<SampleMSEntity>(tableName, sampleEntity.PartitionKey, sampleEntity.RowKey);
            Assert.AreEqual(sampleEntity.ExtraValue, entity.ExtraValue);
        }

        [Test]
        [ExpectedException(typeof(TableNotFoundAzureException))]
        public void InsertOrReplaceEntity_InvalidTable_ExpectsException()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A",
                ExtraValue = "Extra"
            };

            client.InsertOrReplaceEntity(tableName, sampleEntity);

            // expects exception
        }


        [Test]
        public void InsertOrMergeEntity_ValidTable_InsertsEntityInTable()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A"
            };

            client.InsertOrMergeEntity(tableName, sampleEntity);

            _util.AssertEntityExists(tableName, sampleEntity);
        }
        [Test]
        public async Task InsertOrMergeEntityAsync_ValidTable_InsertsEntityInTable()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            _util.CreateTable(tableName);
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A"
            };

            await client.InsertOrMergeEntityAsync(tableName, sampleEntity);

            _util.AssertEntityExists(tableName, sampleEntity);
        }

        [Test]
        public void InsertOrMergeEntity_ExistingEntity_UpdatesEntityInTable()
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

            client.InsertOrMergeEntity(tableName, sampleEntity);

            _util.AssertEntityExists(tableName, sampleEntity);
            var entity = _util.GetEntity<SampleMSEntity>(tableName, sampleEntity.PartitionKey, sampleEntity.RowKey);
            Assert.AreEqual(sampleEntity.ExtraValue, entity.ExtraValue);
        }
        [Test]
        public async Task InsertOrMergeEntityAsync_ExistingEntity_UpdatesEntityInTable()
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

            await client.InsertOrMergeEntityAsync(tableName, sampleEntity);

            _util.AssertEntityExists(tableName, sampleEntity);
            var entity = _util.GetEntity<SampleMSEntity>(tableName, sampleEntity.PartitionKey, sampleEntity.RowKey);
            Assert.AreEqual(sampleEntity.ExtraValue, entity.ExtraValue);
        }

        [Test]
        [ExpectedException(typeof(TableNotFoundAzureException))]
        public void InsertOrMergeEntity_InvalidTable_ExpectsException()
        {
            ITableServiceClient client = new TableServiceClient(_accountSettings);
            var tableName = _util.GenerateSampleTableName();
            var sampleEntity = new SampleEntity()
            {
                PartitionKey = "1",
                RowKey = "A",
                ExtraValue = "Extra"
            };

            client.InsertOrMergeEntity(tableName, sampleEntity);

            // expects exception
        }

    }
}
