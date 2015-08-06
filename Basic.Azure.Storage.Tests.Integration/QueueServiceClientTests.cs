using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Queue.Protocol;
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

        private string GenerateSampleQueueName(string prefix = "")
        {
            var name = prefix + "unit-test-" + Guid.NewGuid().ToString().ToLower();
            _queuesToCleanUp.Add(name);
            return name;
        }

        public string FakePopReceipt { get { return "AAAA/AAAAAAAAAAA"; } }

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

        #region Account Operations

        [Test]
        public void ListQueues_AtLeastOneQueue_ReturnsListContainingThatQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            var response = client.ListQueues();

            Assert.IsTrue(response.Queues.Any(q => q.Name == queueName));
        }

        [Test]
        public void ListQueues_WithPrefix_ReturnsListContainingOnlyQueuesWithThatPrefix()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueNames = new List<string>();
            for (int i = 18; i < 21; i++)
            {
                var queueName = GenerateSampleQueueName(i.ToString());
                CreateQueue(queueName);
                queueNames.Add(queueName);
            }

            var response = client.ListQueues("1");

            Assert.AreEqual("1", response.Prefix);
            Assert.IsTrue(response.Queues.Count(q => q.Name.StartsWith("1")) >= 2);
            Assert.AreEqual(0, response.Queues.Count(q => !q.Name.StartsWith("1")));
        }

        [Test]
        public void ListQueues_MaxResultsSmallerThanQueueList_ReturnsOnlyThatManyResults()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueNames = new List<string>();
            for (int i = 0; i < 5; i++)
            {
                var queueName = GenerateSampleQueueName(i.ToString());
                CreateQueue(queueName);
                queueNames.Add(queueName);
            }

            var response = client.ListQueues(maxResults: 3);

            Assert.AreEqual(3, response.Queues.Count);
        }

        [Test]
        public void ListQueues_WithContinuationMarker_ReturnsRemainderOfList()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueNames = new List<string>();
            for (int i = 0; i < 5; i++)
            {
                var queueName = GenerateSampleQueueName(i.ToString());
                CreateQueue(queueName);
                queueNames.Add(queueName);
            }
            var response = client.ListQueues(maxResults: 3);

            var response2 = client.ListQueues(marker: response.Marker);

            Assert.IsNotEmpty(response2.Queues);
            Assert.GreaterOrEqual(response2.Queues.Count, 2);
        }

        [Test]
        public void ListQueues_IncludingMetadata_ReturnsQueuesWithMetadata()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName, new Dictionary<string, string>() { 
                {"a", "1"},
                {"b", "2"}
            });

            var response = client.ListQueues(includeMetadata: true);

            Assert.IsTrue(response.Queues.Any(q => q.Name == queueName));
            var queue = response.Queues.Where(q => q.Name == queueName).Single();
            Assert.IsNotEmpty(queue.Metadata);
            Assert.AreEqual("1", queue.Metadata["a"]);
            Assert.AreEqual("2", queue.Metadata["b"]);
        }

        [Test]
        public async Task ListQueuesAsync_AtLeastOneQueue_ReturnsListContainingThatQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            var response = await client.ListQueuesAsync();

            Assert.IsTrue(response.Queues.Any(q => q.Name == queueName));
        }

        [Test]
        public void SetQueueServiceProperties_TurnOffLoggingAndMetrics_SuccessfullyTurnsOffOptionsOnService()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var expectedServiceProperties = new StorageServiceProperties();
            expectedServiceProperties.Logging = new StorageServiceLoggingProperties()
            {
                Delete = false,
                Read = false,
                Write = false,
                RetentionPolicyEnabled = false
            };
            expectedServiceProperties.Metrics = new StorageServiceMetricsProperties()
            {
                Enabled = false,
                RetentionPolicyEnabled = false
            };
            SetServicePropertiesOn();

            client.SetQueueServiceProperties(expectedServiceProperties);

            var cloudClient = _storageAccount.CreateCloudQueueClient();
            var actualProperties = cloudClient.GetServiceProperties();
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Shared.Protocol.LoggingOperations.None, actualProperties.Logging.LoggingOperations);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Shared.Protocol.MetricsLevel.None, actualProperties.HourMetrics.MetricsLevel);
        }

        [Test]
        public void SetQueueServiceProperties_TurnOnLoggingAndMetrics_SuccessfullyTurnsOnOptionsOnService()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var expectedServiceProperties = new StorageServiceProperties();
            expectedServiceProperties.Logging = new StorageServiceLoggingProperties()
            {
                Delete = true,
                Read = true,
                Write = true,
                RetentionPolicyEnabled = true,
                RetentionPolicyNumberOfDays = 123
            };
            expectedServiceProperties.Metrics = new StorageServiceMetricsProperties()
            {
                Enabled = true,
                IncludeAPIs = true,
                RetentionPolicyEnabled = true,
                RetentionPolicyNumberOfDays = 45
            };
            SetServicePropertiesOff();

            client.SetQueueServiceProperties(expectedServiceProperties);

            var cloudClient = _storageAccount.CreateCloudQueueClient();
            var actualProperties = cloudClient.GetServiceProperties();
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Shared.Protocol.LoggingOperations.All, actualProperties.Logging.LoggingOperations);
            Assert.AreEqual(123, actualProperties.Logging.RetentionDays);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Shared.Protocol.MetricsLevel.ServiceAndApi, actualProperties.HourMetrics.MetricsLevel);
            Assert.AreEqual(45, actualProperties.HourMetrics.RetentionDays);
        }

        [Test]
        public async Task SetQueueServicePropertiesAsync_TurnOnLoggingAndMetrics_SuccessfullyTurnsOnOptionsOnService()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var expectedServiceProperties = new StorageServiceProperties();
            expectedServiceProperties.Logging = new StorageServiceLoggingProperties()
            {
                Delete = true,
                Read = true,
                Write = true,
                RetentionPolicyEnabled = true,
                RetentionPolicyNumberOfDays = 123
            };
            expectedServiceProperties.Metrics = new StorageServiceMetricsProperties()
            {
                Enabled = true,
                IncludeAPIs = true,
                RetentionPolicyEnabled = true,
                RetentionPolicyNumberOfDays = 45
            };
            SetServicePropertiesOff();

            await client.SetQueueServicePropertiesAsync(expectedServiceProperties);

            var cloudClient = _storageAccount.CreateCloudQueueClient();
            var actualProperties = cloudClient.GetServiceProperties();
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Shared.Protocol.LoggingOperations.All, actualProperties.Logging.LoggingOperations);
            Assert.AreEqual(123, actualProperties.Logging.RetentionDays);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Shared.Protocol.MetricsLevel.ServiceAndApi, actualProperties.HourMetrics.MetricsLevel);
            Assert.AreEqual(45, actualProperties.HourMetrics.RetentionDays);
        }

        [Test]
        public void GetQueueServiceProperties_EverythingEnabled_RetrievesPropertiesSuccessfully()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            SetServicePropertiesOn();

            var response = client.GetQueueServiceProperties();

            Assert.IsTrue(response.Properties.Logging.Delete);
            Assert.IsTrue(response.Properties.Logging.Read);
            Assert.IsTrue(response.Properties.Logging.Write);
            Assert.IsTrue(response.Properties.Logging.RetentionPolicyEnabled);
            Assert.IsTrue(response.Properties.Metrics.Enabled);
            Assert.IsTrue(response.Properties.Metrics.IncludeAPIs);
            Assert.IsTrue(response.Properties.Metrics.RetentionPolicyEnabled);
        }

        [Test]
        public void GetQueueServiceProperties_EverythingDisabled_RetrievesPropertiesSuccessfully()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            SetServicePropertiesOff();

            var response = client.GetQueueServiceProperties();

            Assert.IsFalse(response.Properties.Logging.Delete);
            Assert.IsFalse(response.Properties.Logging.Read);
            Assert.IsFalse(response.Properties.Logging.Write);
            Assert.IsFalse(response.Properties.Logging.RetentionPolicyEnabled);
            Assert.IsFalse(response.Properties.Metrics.Enabled);
            Assert.IsFalse(response.Properties.Metrics.IncludeAPIs);
            Assert.IsFalse(response.Properties.Metrics.RetentionPolicyEnabled);
        }

        [Test]
        public async Task GetQueueServicePropertiesAsync_EverythingEnabled_RetrievesPropertiesSuccessfully()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            SetServicePropertiesOn();

            var response = await client.GetQueueServicePropertiesAsync();

            Assert.IsTrue(response.Properties.Logging.Delete);
            Assert.IsTrue(response.Properties.Logging.Read);
            Assert.IsTrue(response.Properties.Logging.Write);
            Assert.IsTrue(response.Properties.Logging.RetentionPolicyEnabled);
            Assert.IsTrue(response.Properties.Metrics.Enabled);
            Assert.IsTrue(response.Properties.Metrics.IncludeAPIs);
            Assert.IsTrue(response.Properties.Metrics.RetentionPolicyEnabled);
        }

        #endregion

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
            DateTime expectedStart = GetTruncatedUtcNow();
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
            DateTime expectedStart = GetTruncatedUtcNow();
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

        [Test]
        public void SetQueueACL_ValidQueue_AddsPolicyToQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            var expectedStartTime = GetTruncatedUtcNow();
            var expectedIdentifier = new QueueSignedIdentifier()
            {
                Id = "abc-123",
                AccessPolicy = new QueueAccessPolicy()
                {
                    StartTime = expectedStartTime,
                    Expiry = expectedStartTime.AddHours(1),
                    Permission = QueueSharedAccessPermissions.Add
                }
            };

            client.SetQueueACL(queueName, new List<QueueSignedIdentifier>() { expectedIdentifier });

            var actual = GetQueuePermissions(queueName);
            Assert.AreEqual(1, actual.SharedAccessPolicies.Count);
            AssertIdentifierInSharedAccessPolicies(actual.SharedAccessPolicies, expectedIdentifier);
        }

        [Test]
        public void SetQueueACL_ValidQueueAndMultipledentifiers_AddsPoliciesToQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            var expectedStartTime = GetTruncatedUtcNow();
            var expectedIdentifiers = new List<QueueSignedIdentifier>(){
                new QueueSignedIdentifier()
                {
                    Id = "abc-123a",
                    AccessPolicy = new QueueAccessPolicy()
                    {
                        StartTime = expectedStartTime.AddMinutes(12),
                        Expiry = expectedStartTime.AddHours(1),
                        Permission = QueueSharedAccessPermissions.Add
                    }
                },
                new QueueSignedIdentifier()
                {
                    Id = "abc-123b",
                    AccessPolicy = new QueueAccessPolicy()
                    {
                        StartTime = expectedStartTime.AddMinutes(34),
                        Expiry = expectedStartTime.AddHours(1),
                        Permission = QueueSharedAccessPermissions.Add
                    }
                }
            };

            client.SetQueueACL(queueName, expectedIdentifiers);

            var actual = GetQueuePermissions(queueName);
            Assert.AreEqual(2, actual.SharedAccessPolicies.Count);
            foreach (var expectedIdentifier in expectedIdentifiers)
            {
                AssertIdentifierInSharedAccessPolicies(actual.SharedAccessPolicies, expectedIdentifier);
            }
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public void SetQueueACL_NonExistentQueue_ThrowsQueueNotFoundException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            var expectedStartTime = GetTruncatedUtcNow();
            var expectedIdentifier = new QueueSignedIdentifier()
            {
                Id = "abc-123",
                AccessPolicy = new QueueAccessPolicy()
                {
                    StartTime = expectedStartTime,
                    Expiry = expectedStartTime.AddHours(1),
                    Permission = QueueSharedAccessPermissions.Add
                }
            };

            client.SetQueueACL(queueName, new List<QueueSignedIdentifier>() { expectedIdentifier });

            //expects exception
        }

        [Test]
        public async Task SetQueueACLAsync_ValidQueue_AddsPolicyToQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            var expectedStartTime = GetTruncatedUtcNow();
            var expectedIdentifier = new QueueSignedIdentifier()
            {
                Id = "abc-123",
                AccessPolicy = new QueueAccessPolicy()
                {
                    StartTime = expectedStartTime,
                    Expiry = expectedStartTime.AddHours(1),
                    Permission = QueueSharedAccessPermissions.Add
                }
            };

            await client.SetQueueACLAsync(queueName, new List<QueueSignedIdentifier>() { expectedIdentifier });

            var actual = GetQueuePermissions(queueName);
            Assert.AreEqual(1, actual.SharedAccessPolicies.Count);
            AssertIdentifierInSharedAccessPolicies(actual.SharedAccessPolicies, expectedIdentifier);
        }


        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public async Task SetQueueACLAsync_NonExistentQueue_ThrowsQueueNotFoundException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            var expectedStartTime = GetTruncatedUtcNow();
            var expectedIdentifier = new QueueSignedIdentifier()
            {
                Id = "abc-123",
                AccessPolicy = new QueueAccessPolicy()
                {
                    StartTime = expectedStartTime,
                    Expiry = expectedStartTime.AddHours(1),
                    Permission = QueueSharedAccessPermissions.Add
                }
            };

            await client.SetQueueACLAsync(queueName, new List<QueueSignedIdentifier>() { expectedIdentifier });

            //expects exception
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
        [ExpectedException(typeof(RequestBodyTooLargeAzureException))]
        public void PutMessage_TooLargeMessage_ThrowsRequestBodyTooLargeException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            string message = new String('a', 64 * 1024 + 1);

            client.PutMessage(queueName, message);

            //expects exception
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
            Thread.Sleep(3100); // a little extra to be sure
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


        [Test]
        public void GetMessages_EmptyQueue_ReturnsEmptyCollection()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            var response = client.GetMessages(queueName, 32);

            Assert.IsEmpty(response.Messages);
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public void GetMessages_NonExistentQueue_ThrowsQueueDoesNotExistException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            var response = client.GetMessages(queueName, 32);

            // expects exception
        }

        [Test]
        public void GetMessages_Request32ItemsFromFullQueue_Returns32Items()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            AddItemsToQueue(queueName, Enumerable.Range(1, 40).Select(n => n.ToString()).ToList());

            var response = client.GetMessages(queueName, 32);

            Assert.AreEqual(32, response.Messages.Count);
            for (int i = 1; i <= 32; i++)
            {
                // Base 64 encode the expected message since Azure SDK did so when enqueueing it
                var expectedMessage = Convert.ToBase64String(ASCIIEncoding.ASCII.GetBytes(i.ToString()));
                Assert.IsTrue(response.Messages.Any(m => m.MessageText.Equals(expectedMessage)));
            }
        }

        [Test]
        public void GetMessages_RequestUndefinedNumberOfItemsFromFullQueue_ReturnsOneItem()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            AddItemsToQueue(queueName, Enumerable.Range(1, 40).Select(n => n.ToString()).ToList());
            var expectedMessage = "1";

            var response = client.GetMessages(queueName);

            Assert.AreEqual(1, response.Messages.Count);
            var message = response.Messages.Single();
            // Base 64 encode the expected message since Azure SDK did so when enqueueing it
            Assert.AreEqual(Convert.ToBase64String(ASCIIEncoding.ASCII.GetBytes(expectedMessage)), message.MessageText);
        }

        [Test]
        public void GetMessages_RequestItemWithVisibility_ReturnsItemWithFutureVisibility()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            AddItemsToQueue(queueName, new List<string>() { "1" });

            var response = client.GetMessages(queueName, 1, 30);

            Assert.AreEqual(1, response.Messages.Count);
            var message = response.Messages.Single();
            Assert.Less(message.InsertionTime, DateTime.UtcNow);
            Assert.Greater(message.ExpirationTime, DateTime.UtcNow);
            Assert.Greater(message.TimeNextVisible, DateTime.UtcNow);
        }

        [Test]
        public void GetMessages_RequestItemFromPopulatedQueue_ReturnsItemWithPopReceiptAndDequeueCount()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            AddItemsToQueue(queueName, new List<string>() { "1" });

            var response = client.GetMessages(queueName, 1, 30);

            Assert.AreEqual(1, response.Messages.Count);
            var message = response.Messages.Single();
            Assert.IsNotNullOrEmpty(message.PopReceipt);
            Assert.Greater(message.DequeueCount, 0);
        }


        [Test]
        public async Task GetMessagesAsync_EmptyQueue_ReturnsEmptyCollection()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            var response = await client.GetMessagesAsync(queueName, 32);

            Assert.IsEmpty(response.Messages);
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public async Task GetMessagesAsync_NonExistentQueue_ThrowsQueueDoesNotExistException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            var response = await client.GetMessagesAsync(queueName, 32);

            // expects exception
        }

        [Test]
        public async Task GetMessagesAsync_Request32ItemsFromFullQueue_Returns32Items()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            AddItemsToQueue(queueName, Enumerable.Range(1, 40).Select(n => n.ToString()).ToList());

            var response = await client.GetMessagesAsync(queueName, 32);

            Assert.AreEqual(32, response.Messages.Count);
            for (int i = 1; i <= 32; i++)
            {
                // Base 64 encode the expected message since Azure SDK did so when enqueueing it
                var expectedMessage = Convert.ToBase64String(ASCIIEncoding.ASCII.GetBytes(i.ToString()));
                Assert.IsTrue(response.Messages.Any(m => m.MessageText.Equals(expectedMessage)));
            }
        }

        [Test]
        public void PeekMessages_EmptyQueue_ReturnsEmptyCollection()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            var response = client.PeekMessages(queueName, 32);

            Assert.IsEmpty(response.Messages);
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public void PeekMessages_NonExistentQueue_ThrowsQueueDoesNotExistException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            var response = client.PeekMessages(queueName, 32);

            // expects exception
        }

        [Test]
        public void PeekMessages_Request32ItemsFromFullQueue_Returns32Items()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            AddItemsToQueue(queueName, Enumerable.Range(1, 40).Select(n => n.ToString()).ToList());

            var response = client.PeekMessages(queueName, 32);

            Assert.AreEqual(32, response.Messages.Count);
            for (int i = 1; i <= 32; i++)
            {
                // Base 64 encode the expected message since Azure SDK did so when enqueueing it
                var expectedMessage = Convert.ToBase64String(ASCIIEncoding.ASCII.GetBytes(i.ToString()));
                Assert.IsTrue(response.Messages.Any(m => m.MessageText.Equals(expectedMessage)));
            }
        }

        [Test]
        public void PeekMessages_RequestUndefinedNumberOfItemsFromFullQueue_ReturnsOneItem()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            AddItemsToQueue(queueName, Enumerable.Range(1, 40).Select(n => n.ToString()).ToList());
            var expectedMessage = "1";

            var response = client.PeekMessages(queueName);

            Assert.AreEqual(1, response.Messages.Count);
            var message = response.Messages.Single();
            // Base 64 encode the expected message since Azure SDK did so when enqueueing it
            Assert.AreEqual(Convert.ToBase64String(ASCIIEncoding.ASCII.GetBytes(expectedMessage)), message.MessageText);
        }

        [Test]
        public void PeekMessages_RequestItem_DoesNotDequeueTheItem()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            AddItemsToQueue(queueName, new List<string>() { "1" });

            var peekResponse = client.PeekMessages(queueName, 1);
            Assert.AreEqual(1, peekResponse.Messages.Count);

            var getResponse = client.GetMessages(queueName, 1);
            Assert.AreEqual(1, getResponse.Messages.Count);
        }

        [Test]
        public async Task PeekMessagesAsync_EmptyQueue_ReturnsEmptyCollection()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            var response = await client.PeekMessagesAsync(queueName, 32);

            Assert.IsEmpty(response.Messages);
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public async Task PeekMessagesAsync_NonExistentQueue_ThrowsQueueDoesNotExistException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            var response = await client.PeekMessagesAsync(queueName, 32);

            // expects exception
        }

        [Test]
        public void DeleteMessage_ValidMessage_DeletesItFromTheQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            AddItemsToQueue(queueName, new List<string>() { "1" });
            var itemFromQueue = GetItemFromQueue(queueName);

            client.DeleteMessage(queueName, itemFromQueue.Id, itemFromQueue.PopReceipt);

            AssertQueueIsEmpty(queueName);
        }

        [Test]
        [ExpectedException(typeof(MessageNotFoundAzureException))]
        public void DeleteMessage_NonexistentMessage_ThrowsMessageNotFoundException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            client.DeleteMessage(queueName, "abc-123", FakePopReceipt);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(InvalidQueryParameterValueAzureException))]
        public void DeleteMessage_BadlyFormattedPopReceipt_ThrowsInvalidQueryParameterException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            client.DeleteMessage(queueName, "abc-123", "bad format");

            // expects exception
        }

        [Test]
        public async Task DeleteMessageAsync_ValidMessage_DeletesItFromTheQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            AddItemsToQueue(queueName, new List<string>() { "1" });
            var itemFromQueue = GetItemFromQueue(queueName);

            await client.DeleteMessageAsync(queueName, itemFromQueue.Id, itemFromQueue.PopReceipt);

            AssertQueueIsEmpty(queueName);
        }

        [Test]
        [ExpectedException(typeof(MessageNotFoundAzureException))]
        public async Task DeleteMessageAsync_NonexistentMessage_ThrowsMessageNotFoundException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            await client.DeleteMessageAsync(queueName, "abc-123", FakePopReceipt);

            // expects exception
        }

        [Test]
        public void ClearMessages_MessagesInQueue_LeavesQueueEmpty()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            AddItemsToQueue(queueName, new List<string>() { "1", "2", "3", "4" });

            client.ClearMessages(queueName);

            AssertQueueIsEmpty(queueName);
        }

        [Test]
        public void ClearMessages_NoMessagesInQueue_LeavesQueueEmpty()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            client.ClearMessages(queueName);

            AssertQueueIsEmpty(queueName);
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public void ClearMessages_QueueDoesNotExist_ThrowsQueueNotFoundException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            client.ClearMessages(queueName);

            //expects exception
        }

        [Test]
        public async Task ClearMessagesAsync_MessagesInQueue_LeavesQueueEmpty()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            AddItemsToQueue(queueName, new List<string>() { "1", "2", "3", "4" });

            await client.ClearMessagesAsync(queueName);

            AssertQueueIsEmpty(queueName);
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public async Task ClearMessagesAsync_QueueDoesNotExist_ThrowsQueueNotFoundException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            await client.ClearMessagesAsync(queueName);

            //expects exception
        }

        [Test]
        public void UpdateMessage_ClearVisibility_AllowsMessageToBeRetrievedAgain()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            AddItemsToQueue(queueName, new List<string>() { "1" });
            var itemFromQueue = GetItemFromQueue(queueName);

            client.UpdateMessage(queueName, itemFromQueue.Id, itemFromQueue.PopReceipt, 0);

            var itemFromQueueAgain = GetItemFromQueue(queueName);
            Assert.IsNotNull(itemFromQueueAgain);
            Assert.AreEqual(itemFromQueue.Id, itemFromQueueAgain.Id);
        }

        [Test]
        public void UpdateMessage_UpdateContent_UpdatesContentOfMessageInQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            AddItemsToQueue(queueName, new List<string>() { "1" });
            var itemFromQueue = GetItemFromQueue(queueName);
            var newString = Convert.ToBase64String(Encoding.ASCII.GetBytes("2"));   // the official SDK base 64's the values automatically

            client.UpdateMessage(queueName, itemFromQueue.Id, itemFromQueue.PopReceipt, 0, newString);

            var itemFromQueueAgain = GetItemFromQueue(queueName);
            Assert.IsNotNull(itemFromQueueAgain);
            Assert.AreEqual(itemFromQueue.Id, itemFromQueueAgain.Id);
            Assert.AreEqual("1", itemFromQueue.AsString);
            Assert.AreEqual("2", itemFromQueueAgain.AsString);
        }

        [Test]
        public void UpdateMessage_ExtendVisibility_ExtendsMessageVisibilityInTheQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            AddItemsToQueue(queueName, new List<string>() { "1" });
            var itemFromQueue = GetItemFromQueue(queueName, 1);

            var response = client.UpdateMessage(queueName, itemFromQueue.Id, itemFromQueue.PopReceipt, 30);

            Assert.IsNotNullOrEmpty(response.PopReceipt);
            Thread.Sleep(2000); // longer than the original visibility timeout
            var itemFromQueueAgain = GetItemFromQueue(queueName);
            Assert.IsNull(itemFromQueueAgain);
        }

        [Test]
        [ExpectedException(typeof(MessageNotFoundAzureException))]
        public void UpdateMessage_NonExistentMessage_ThrowsException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            client.UpdateMessage(queueName, "123-abc", FakePopReceipt, 0);

            // expects exception
        }

        [Test]
        public async Task UpdateMessageAsync_ClearVisibility_AllowsMessageToBeRetrievedAgain()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            AddItemsToQueue(queueName, new List<string>() { "1" });
            var itemFromQueue = GetItemFromQueue(queueName);

            await client.UpdateMessageAsync(queueName, itemFromQueue.Id, itemFromQueue.PopReceipt, 0);

            var itemFromQueueAgain = GetItemFromQueue(queueName);
            Assert.IsNotNull(itemFromQueueAgain);
            Assert.AreEqual(itemFromQueue.Id, itemFromQueueAgain.Id);
        }

        [Test]
        [ExpectedException(typeof(MessageNotFoundAzureException))]
        public async Task UpdateMessageAsync_NonExistentMessage_ThrowsException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            await client.UpdateMessageAsync(queueName, "123-abc", FakePopReceipt, 0);

            // expects exception
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

        private void AssertIdentifierInSharedAccessPolicies(Microsoft.WindowsAzure.Storage.Queue.SharedAccessQueuePolicies sharedAccessPolicies, QueueSignedIdentifier expectedIdentifier)
        {
            var policy = sharedAccessPolicies.Where(i => i.Key.Equals(expectedIdentifier.Id, StringComparison.InvariantCultureIgnoreCase)).FirstOrDefault();
            Assert.IsNotNull(policy);
            Assert.AreEqual(expectedIdentifier.AccessPolicy.StartTime, policy.Value.SharedAccessStartTime.Value.UtcDateTime);
            Assert.AreEqual(expectedIdentifier.AccessPolicy.Expiry, policy.Value.SharedAccessExpiryTime.Value.UtcDateTime);
            Assert.IsTrue(policy.Value.Permissions.HasFlag(Microsoft.WindowsAzure.Storage.Queue.SharedAccessQueuePermissions.Add));
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

        private void AddItemsToQueue(string queueName, List<string> messages)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);

            foreach (var message in messages)
            {
                queue.AddMessage(new Microsoft.WindowsAzure.Storage.Queue.CloudQueueMessage(Encoding.ASCII.GetBytes(message)));
            }
        }

        private Microsoft.WindowsAzure.Storage.Queue.CloudQueueMessage GetItemFromQueue(string queueName, int visibility = 30)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);

            return queue.GetMessage(TimeSpan.FromSeconds(visibility));
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

        private QueuePermissions GetQueuePermissions(string queueName)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            var permissions = queue.GetPermissions();
            return permissions;
        }

        private void SetServicePropertiesOn()
        {
            var cloudClient = _storageAccount.CreateCloudQueueClient();
            var actualProperties = cloudClient.GetServiceProperties();
            actualProperties.Logging.LoggingOperations = Microsoft.WindowsAzure.Storage.Shared.Protocol.LoggingOperations.All;
            actualProperties.Logging.RetentionDays = 7;
            actualProperties.HourMetrics.MetricsLevel = Microsoft.WindowsAzure.Storage.Shared.Protocol.MetricsLevel.ServiceAndApi;
            actualProperties.HourMetrics.RetentionDays = 7;
            cloudClient.SetServiceProperties(actualProperties);
        }

        private void SetServicePropertiesOff()
        {
            var cloudClient = _storageAccount.CreateCloudQueueClient();
            var actualProperties = cloudClient.GetServiceProperties();
            actualProperties.Logging.LoggingOperations = Microsoft.WindowsAzure.Storage.Shared.Protocol.LoggingOperations.None;
            actualProperties.Logging.RetentionDays = null;
            actualProperties.HourMetrics.MetricsLevel = Microsoft.WindowsAzure.Storage.Shared.Protocol.MetricsLevel.None;
            actualProperties.HourMetrics.RetentionDays = null;
            cloudClient.SetServiceProperties(actualProperties);
        }

        #endregion
        private DateTime GetTruncatedUtcNow()
        {
            return new DateTime(DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day, DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, DateTime.UtcNow.Second, DateTimeKind.Utc);
        }



    }
}
