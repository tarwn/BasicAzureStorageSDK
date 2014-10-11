using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Tests.Integration
{
    [TestFixture]
    public class QueueServiceClientTests
    {
        private StorageAccountSettings _accountSettings = new LocalEmulatorAccountSettings();
        private CloudStorageAccount _storageAccount = CloudStorageAccount.Parse("UseDevelopmentStorage=true");

        private List<string> _queuesToCleanUp = new List<string>();

        private string GenerateSampleQueueName()
        {
            var name = "unit-test-" + Guid.NewGuid().ToString().ToLower();
            _queuesToCleanUp.Add(name);
            return name;
        }

        [TestFixtureTearDown]
        public void TestFixtureTeardown()
        {
            //let's clean up!
            var client = _storageAccount.CreateCloudQueueClient();
            foreach (var queueName in _queuesToCleanUp)
            {
                var queue = client.GetQueueReference(queueName);
                queue.DeleteIfExists();
            }
        }

        #region Queue Operations Tests

        [Test]
        public void CreateQueue_ValidName_CreatesQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            client.CreateQueue(queueName);

            AssertQueueExists(queueName);
        }

        [Test]
        public void CreateQueue_ValidNameAndMetadata_CreatesQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            client.CreateQueue(queueName, new Dictionary<string, string>() { 
                { "SampleName", "SampleValue" }
            });

            AssertQueueExists(queueName);
        }

        [Test]
        public void CreateQueue_AlreadyExistsWithMatchingMetadata_ReportsNoContentPerDocumentation()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            client.CreateQueue(queueName);

            // I don't think I can/should-be-able-to test the return code at this level...
        }

        [Test]
        [ExpectedException(typeof(QueueAlreadyExistsAzureException))]
        public void CreateQueue_AlreadyExistsWithDifferentMetadata_ReportsConflictProperly()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            client.CreateQueue(queueName, new Dictionary<string, string> { 
                { "SampleKey", "SampleValue" }
            });

            // expects exception
        }

        [Test]
        public async Task CreateQueueAsync_ValidName_CreatesQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            await client.CreateQueueAsync(queueName);

            AssertQueueExists(queueName);
        }

        [Test]
        [ExpectedException(typeof(QueueAlreadyExistsAzureException))]
        public async Task CreateQueueAsync_AlreadyExistsWithDifferentMetadata_ReportsConflictProperly()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            await client.CreateQueueAsync(queueName, new Dictionary<string, string> { 
                { "SampleKey", "SampleValue" }
            });

            // expects exception
        }


        [Test]
        public void DeleteQueue_ValidQueue_DeletesQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            client.DeleteQueue(queueName);

            AssertQueueDoesNotExist(queueName);
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public void DeleteQueue_NonExistentQueue_ReportsError()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            client.DeleteQueue(queueName);

            // expects exception
        }

        [Test]
        public async Task DeleteQueueAsync_ValidQueue_DeletesQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            await client.DeleteQueueAsync(queueName);

            AssertQueueDoesNotExist(queueName);
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public async Task DeleteQueueAsync_NonExistentQueue_ReportsError()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            await client.DeleteQueueAsync(queueName);

            // expects exception
        }


        [Test]
        public void GetQueueMetadata_ValidNameWithEmptyMetadata_ReturnsEmptyMetadata()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            var response = client.GetQueueMetadata(queueName);

            Assert.IsNotNull(response.Metadata);
        }

        [Test]
        public void GetQueueMetadata_ValidNameWithMetadata_ReturnsMetadata()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            var expectedMetadata = new Dictionary<string, string>(){
                {"one", "1"},
                {"two", "2"}
            };
            CreateQueue(queueName, expectedMetadata);

            var response = client.GetQueueMetadata(queueName);

            Assert.IsNotNull(response.Metadata);
            Assert.AreEqual(expectedMetadata.Count, response.Metadata.Count);
            foreach (var key in expectedMetadata.Keys)
            {
                Assert.AreEqual(expectedMetadata[key], response.Metadata[key]);
            }
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public void GetQueueMetadata_NonexistentQueue_ThrowsQueueDoesNotExistException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            var response = client.GetQueueMetadata(queueName);

            // expects exception
        }

        [Test]
        public async Task GetQueueMetadataAsync_ValidNameWithMetadata_ReturnsMetadata()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            var expectedMetadata = new Dictionary<string, string>(){
                {"one", "1"},
                {"two", "2"}
            };
            CreateQueue(queueName, expectedMetadata);

            var response = await client.GetQueueMetadataAsync(queueName);

            Assert.IsNotNull(response.Metadata);
            Assert.AreEqual(expectedMetadata.Count, response.Metadata.Count);
            foreach (var key in expectedMetadata.Keys)
            {
                Assert.AreEqual(expectedMetadata[key], response.Metadata[key]);
            }
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public async Task GetQueueMetadataAsync_NonexistentQueue_ThrowsQueueDoesNotExistException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            var response = await client.GetQueueMetadataAsync(queueName);

            // expects exception
        }

        [Test]
        public void SetQueueMetadata_EmptyMetadata_SetsEmptyMetadataOnQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            client.SetQueueMetadata(queueName, new Dictionary<string, string>());

            var metadata = GetQueueMetadata(queueName);
            Assert.IsEmpty(metadata);
        }

        [Test]
        public void SetQueueMetadata_ValidMetadata_SetsMetadataOnQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            var expectedMetadata = new Dictionary<string, string>(){
                {"one", "1"},
                {"two", "2"}
            };

            client.SetQueueMetadata(queueName, expectedMetadata);

            var metadata = GetQueueMetadata(queueName);
            Assert.IsNotNull(metadata);
            Assert.AreEqual(expectedMetadata.Count, metadata.Count);
            foreach (var key in expectedMetadata.Keys)
            {
                Assert.AreEqual(expectedMetadata[key], metadata[key]);
            }
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public void SetQueueMetadata_NonexistentQueue_ThrowsQueueDoesNotExistException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            client.SetQueueMetadata(queueName, new Dictionary<string, string>());

            // expects exception
        }

        [Test]
        public async Task SetQueueMetadataAsync_ValidMetadata_SetsMetadataOnQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            var expectedMetadata = new Dictionary<string, string>(){
                {"one", "1"},
                {"two", "2"}
            };

            await client.SetQueueMetadataAsync(queueName, expectedMetadata);

            var metadata = GetQueueMetadata(queueName);
            Assert.IsNotNull(metadata);
            Assert.AreEqual(expectedMetadata.Count, metadata.Count);
            foreach (var key in expectedMetadata.Keys)
            {
                Assert.AreEqual(expectedMetadata[key], metadata[key]);
            }
        }
        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public async Task SetQueueMetadataAsync_NonexistentQueue_ThrowsQueueDoesNotExistException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            await client.SetQueueMetadataAsync(queueName, new Dictionary<string, string>());

            // expects exception
        }

        [Test]
        public void GetQueueACL_NoAccessPolicies_ReturnsEmptyList()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            var result = client.GetQueueACL(queueName);

            Assert.IsEmpty(result.SignedIdentifiers);
        }

        [Test]
        public void GetQueueACL_HasAccessPolicies_ReturnsListConstainingThosePolicies()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            string expectedId = "abc-123";
            DateTime expectedStart = getTruncatedUtcNow();
            AddAccessPolicy(queueName, expectedId, expectedStart, expectedStart.AddDays(1));

            var result = client.GetQueueACL(queueName);

            Assert.IsNotEmpty(result.SignedIdentifiers);
            Assert.AreEqual("abc-123", result.SignedIdentifiers.First().Id);
            Assert.AreEqual(expectedStart, result.SignedIdentifiers.First().AccessPolicy.StartTime);
            Assert.AreEqual(expectedStart.AddDays(1), result.SignedIdentifiers.First().AccessPolicy.Expiry);
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public void GetQueueACL_NonexistentQueue_ThrowsQueueNotFoundException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            var result = client.GetQueueACL(queueName);

            // expects exception
        }

        [Test]
        public async Task GetQueueACLAsync_HasAccessPolicies_ReturnsListConstainingThosePolicies()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            string expectedId = "abc-123";
            DateTime expectedStart = getTruncatedUtcNow();
            AddAccessPolicy(queueName, expectedId, expectedStart, expectedStart.AddDays(1));

            var result = await client.GetQueueACLAsync(queueName);

            Assert.IsNotEmpty(result.SignedIdentifiers);
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public async Task GetQueueACLAync_NonexistentQueue_ThrowsQueueNotFoundException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            var result = await client.GetQueueACLAsync(queueName);

            // expects exception
        }
        #endregion

        #region Message Operations Tests

        [Test]
        public void PutMessage_ValidMessage_AddsMessageToQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            string message = "Unit Test Message";

            client.PutMessage(queueName, message);

            AssertQueueHasMessage(queueName);
        }

        [Test]
        public void PutMessage_ValidMessageWithVisibilityTimeout_IsNotVisibleInQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            string message = "Unit Test Message";

            client.PutMessage(queueName, message, visibilityTimeout: 5);

            AssertQueueInvisibleMessage(queueName);
        }


        [Test]
        [Ignore("Either I messed up the MessageTTL property or it doesn't work as expected")]
        public void PutMessage_ValidMessageWithTTL_DisappearsAfterTTLExpires()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            string message = "Unit Test Message";

            client.PutMessage(queueName, message, messageTtl: 1);

            AssertQueueHasMessage(queueName);
            Thread.Sleep(1100); // a little extra to be sure
            AssertQueueIsEmpty(queueName);
        }

        [Test]
        public async Task PutMessageAsync_ValidMessage_AddsMessageToQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            string message = "Unit Test Message";

            await client.PutMessageAsync(queueName, message);

            AssertQueueHasMessage(queueName);
        }

        #endregion

        #region Assertions

        private void AssertQueueExists(string queueName)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            if (!queue.Exists())
            {
                Assert.Fail(String.Format("The queue '{0}' does not exist", queueName));
            }
        }

        private void AssertQueueDoesNotExist(string queueName)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            if (queue.Exists())
            {
                Assert.Fail(String.Format("The queue '{0}' exists", queueName));
            }
        }

        private void AssertQueueHasMessage(string queueName)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            if (!queue.Exists())
            {
                Assert.Fail(String.Format("The queue '{0}' does not exist", queueName));
            }

            var msg = queue.PeekMessage();
            if (msg == null)
            {
                Assert.Fail(String.Format("The queue '{0}' does not have any messages", queueName));
            }
        }

        private void AssertQueueIsEmpty(string queueName)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            if (!queue.Exists())
            {
                Assert.Fail(String.Format("The queue '{0}' does not exist", queueName));
            }

            queue.FetchAttributes();

            if (queue.ApproximateMessageCount > 0)
            {
                Assert.Fail(String.Format("The queue '{0}' reports {1} messages", queueName, queue.ApproximateMessageCount));
            }
        }

        private void AssertQueueInvisibleMessage(string queueName)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            if (!queue.Exists())
            {
                Assert.Fail(String.Format("The queue '{0}' does not exist", queueName));
            }

            var msg = queue.PeekMessage();
            queue.FetchAttributes();

            if (msg != null)
            {
                Assert.Fail(String.Format("The queue '{0}' had a visible message", queueName));
            }

            if (queue.ApproximateMessageCount == 0)
            {
                Assert.Fail(String.Format("The queue '{0}' reports 0 ApproximateMessageCount", queueName));
            }
        }

        #endregion

        #region Setup Mechanics

        private void CreateQueue(string queueName, Dictionary<string, string> metadata = null)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            queue.Create();

            if (metadata != null)
            {
                // tell me how this method of adding metadata makes any sense at all?
                foreach (var key in metadata.Keys)
                {
                    queue.Metadata.Add(key, metadata[key]);
                }
                queue.SetMetadata();
            }
        }

        private IDictionary<string, string> GetQueueMetadata(string queueName)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);

            queue.FetchAttributes();
            return queue.Metadata;
        }

        private void AddAccessPolicy(string queueName, string id, DateTime startDate, DateTime expiry)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            var permissions = queue.GetPermissions();
            permissions.SharedAccessPolicies.Add(id, new Microsoft.WindowsAzure.Storage.Queue.SharedAccessQueuePolicy()
            {
                Permissions = Microsoft.WindowsAzure.Storage.Queue.SharedAccessQueuePermissions.Add,
                SharedAccessStartTime = startDate,
                SharedAccessExpiryTime = expiry
            });
            queue.SetPermissions(permissions);
        }

        #endregion
        private DateTime getTruncatedUtcNow()
        {
            return new DateTime(DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day, DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, DateTime.UtcNow.Second, DateTimeKind.Utc);
        }

    }
}
