using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Queue.Protocol;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Tests.Integration.QueueServiceClientTests
{
    [TestFixture]
    public class QueueOperationsTests
    {
        private StorageAccountSettings _accountSettings = StorageAccountSettings.Parse(ConfigurationManager.AppSettings["AzureConnectionString"]);
        private QueueUtil _util = new QueueUtil(ConfigurationManager.AppSettings["AzureConnectionString"]);

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
            _util.DeleteIfExists(_queuesToCleanUp);
        }

        #region Queue Operations Tests

        [Test]
        public void CreateQueue_ValidName_CreatesQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            client.CreateQueue(queueName);

            _util.AssertQueueExists(queueName);
        }

        [Test]
        public void CreateQueue_ValidNameAndMetadata_CreatesQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            client.CreateQueue(queueName, new Dictionary<string, string>() { 
                { "SampleName", "SampleValue" }
            });

            _util.AssertQueueExists(queueName);
        }

        [Test]
        public void CreateQueue_AlreadyExistsWithNoMetadata_ReportsNoContentPerDocumentation()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);

            client.CreateQueue(queueName);

            // I don't think I can/should-be-able-to test the return code at this level...
        }

        [Test]
        public void CreateQueue_AlreadyExistsWithMatchingMetadata_ReportsNoContentPerDocumentation()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName, new Dictionary<string, string> { 
                { "SampleKey", "SampleValue" }
            });

            client.CreateQueue(queueName, new Dictionary<string, string> { 
                { "SampleKey", "SampleValue" }
            });

            // I don't think I can/should-be-able-to test the return code at this level...
        }

        [Test]
        [ExpectedException(typeof(QueueAlreadyExistsAzureException))]
        public void CreateQueue_AlreadyExistsWithDifferentMetadata_ReportsConflictProperly()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName, new Dictionary<string, string> { 
                { "SampleKey", "SampleValue" }
            });

            client.CreateQueue(queueName, new Dictionary<string, string> { 
                { "SampleKey", "SampleValue2" }
            });

            // expects exception
        }

        [Test]
        public async Task CreateQueueAsync_ValidName_CreatesQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            await client.CreateQueueAsync(queueName);

            _util.AssertQueueExists(queueName);
        }

        [Test]
        [ExpectedException(typeof(QueueAlreadyExistsAzureException))]
        public async Task CreateQueueAsync_AlreadyExistsWithDifferentMetadata_ReportsConflictProperly()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);

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
            _util.CreateQueue(queueName);

            client.DeleteQueue(queueName);

            _util.AssertQueueDoesNotExist(queueName);
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
            _util.CreateQueue(queueName);

            await client.DeleteQueueAsync(queueName);

            _util.AssertQueueDoesNotExist(queueName);
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
            _util.CreateQueue(queueName);

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
            _util.CreateQueue(queueName, expectedMetadata);

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
            _util.CreateQueue(queueName, expectedMetadata);

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
            _util.CreateQueue(queueName);

            client.SetQueueMetadata(queueName, new Dictionary<string, string>());

            var metadata = _util.GetQueueMetadata(queueName);
            Assert.IsEmpty(metadata);
        }

        [Test]
        public void SetQueueMetadata_ValidMetadata_SetsMetadataOnQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            var expectedMetadata = new Dictionary<string, string>(){
                {"one", "1"},
                {"two", "2"}
            };

            client.SetQueueMetadata(queueName, expectedMetadata);

            var metadata = _util.GetQueueMetadata(queueName);
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
            _util.CreateQueue(queueName);
            var expectedMetadata = new Dictionary<string, string>(){
                {"one", "1"},
                {"two", "2"}
            };

            await client.SetQueueMetadataAsync(queueName, expectedMetadata);

            var metadata = _util.GetQueueMetadata(queueName);
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
            _util.CreateQueue(queueName);

            var result = client.GetQueueACL(queueName);

            Assert.IsEmpty(result.SignedIdentifiers);
        }

        [Test]
        public void GetQueueACL_HasAccessPolicies_ReturnsListConstainingThosePolicies()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            string expectedId = "abc-123";
            DateTime expectedStart = _util.GetTruncatedUtcNow();
            _util.AddAccessPolicy(queueName, expectedId, expectedStart, expectedStart.AddDays(1));

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
            _util.CreateQueue(queueName);
            string expectedId = "abc-123";
            DateTime expectedStart = _util.GetTruncatedUtcNow();
            _util.AddAccessPolicy(queueName, expectedId, expectedStart, expectedStart.AddDays(1));

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
            _util.CreateQueue(queueName);
            var expectedStartTime = _util.GetTruncatedUtcNow();
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

            var actual = _util.GetQueuePermissions(queueName);
            Assert.AreEqual(1, actual.SharedAccessPolicies.Count);
            _util.AssertIdentifierInSharedAccessPolicies(actual.SharedAccessPolicies, expectedIdentifier);
        }

        [Test]
        public void SetQueueACL_ValidQueueAndMultipledentifiers_AddsPoliciesToQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            var expectedStartTime = _util.GetTruncatedUtcNow();
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

            var actual = _util.GetQueuePermissions(queueName);
            Assert.AreEqual(2, actual.SharedAccessPolicies.Count);
            foreach (var expectedIdentifier in expectedIdentifiers)
            {
                _util.AssertIdentifierInSharedAccessPolicies(actual.SharedAccessPolicies, expectedIdentifier);
            }
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public void SetQueueACL_NonExistentQueue_ThrowsQueueNotFoundException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            var expectedStartTime = _util.GetTruncatedUtcNow();
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
            _util.CreateQueue(queueName);
            var expectedStartTime = _util.GetTruncatedUtcNow();
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

            var actual = _util.GetQueuePermissions(queueName);
            Assert.AreEqual(1, actual.SharedAccessPolicies.Count);
            _util.AssertIdentifierInSharedAccessPolicies(actual.SharedAccessPolicies, expectedIdentifier);
        }


        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public async Task SetQueueACLAsync_NonExistentQueue_ThrowsQueueNotFoundException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            var expectedStartTime = _util.GetTruncatedUtcNow();
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

    }
}
