using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Basic.Azure.Storage.Communications.Utility;
using Microsoft.WindowsAzure.Storage.Blob;
using BlobType = Microsoft.WindowsAzure.Storage.Blob.BlobType;
using LeaseDuration = Basic.Azure.Storage.Communications.Common.LeaseDuration;
using LeaseState = Basic.Azure.Storage.Communications.Common.LeaseState;
using LeaseStatus = Basic.Azure.Storage.Communications.Common.LeaseStatus;
using System.Configuration;

namespace Basic.Azure.Storage.Tests.Integration.BlobServiceClientTests
{
    [TestFixture]
    public class ContainerOperationsTests 
    {
        protected const string InvalidLeaseId = "InvalidLeaseId";

        private readonly string _azureConnectionString = ConfigurationManager.AppSettings["AzureConnectionString"];
        private readonly BlobUtil _util = new BlobUtil(ConfigurationManager.AppSettings["AzureConnectionString"]);
        private readonly string _runId = DateTime.Now.ToString("yyyy-MM-dd");

        protected StorageAccountSettings AccountSettings
        {
            get
            {
                return StorageAccountSettings.Parse(_azureConnectionString);
            }
        }

        protected static string FakeLeaseId { get { return "a28cf439-8776-4653-9ce8-4e3df49b4a72"; } }

        protected static DateTime GetTruncatedUtcNow()
        {
            return new DateTime(DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day, DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, DateTime.UtcNow.Second, DateTimeKind.Utc);
        }

        protected static string GetGuidString()
        {
            return Guid.NewGuid().ToString();
        }


        [TestFixtureTearDown]
        public void TestFixtureTeardown()
        {
            //let's clean up!
            _util.Cleanup();
        }

        #region Container Operations Tests

        [Test]
        public void CreateContainer_ValidArguments_CreatesContainerWithSpecificName()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.CreateContainer(containerName, ContainerAccessType.None);

            _util.AssertContainerExists(containerName);
        }

        [Test]
        public void CreateContainer_ValidArgumentsWithPublicContainerAccess_CreatesContainerWithContainerAccess()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.CreateContainer(containerName, ContainerAccessType.PublicContainer);

           _util.AssertContainerAccess(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Container);
        }

        [Test]
        public void CreateContainer_ValidArgumentsWithPublicBlobAccess_CreatesContainerWithBlobAccess()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.CreateContainer(containerName, ContainerAccessType.PublicBlob);

           _util.AssertContainerAccess(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Blob);
        }

        [Test]
        public void CreateContainer_ValidArgumentsWithNoPublicAccess_CreatesContainerWithNoPublicAccess()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.CreateContainer(containerName, ContainerAccessType.None);

           _util.AssertContainerAccess(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off);
        }

        [Test]
        [ExpectedException(typeof(ContainerAlreadyExistsAzureException))]
        public void CreateContainer_AlreadyExists_ThrowsContainerAlreadyExistsException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.CreateContainer(containerName, ContainerAccessType.None);

            // expects exception
        }

        [Test]
        public void CreateContainer_ValidArguments_ReturnsContainerCreationResponse()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.CreateContainer(containerName, ContainerAccessType.None);

            Assert.IsTrue(response.Date > DateTime.Now.AddSeconds(-5), String.Format("Response Date was set to {0}", response.Date));
            Assert.IsTrue(response.LastModified > DateTime.Now.AddSeconds(-5), String.Format("Response LastModified was set to {0}", response.LastModified));
            Assert.IsFalse(string.IsNullOrWhiteSpace(response.ETag), "Response ETag is not set");

        }

        [Test]
        public async Task CreateContainerAsync_ValidArguments_CreatesContainerWithSpecificName()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.CreateContainerAsync(containerName, ContainerAccessType.None);

            _util.AssertContainerExists(containerName);
        }

        [Test]
        [ExpectedException(typeof(ContainerAlreadyExistsAzureException))]
        public async Task CreateContainerAsync_AlreadyExists_ThrowsContainerAlreadyExistsException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.CreateContainerAsync(containerName, ContainerAccessType.None);

            // expects exception
        }

        [Test]
        public void GetContainerProperties_ValidContainer_ReturnsProperties()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            var response = client.GetContainerProperties(containerName);

            Assert.IsNotNull(response.Metadata);
            Assert.IsNotNullOrEmpty(response.ETag);
            Assert.AreEqual(LeaseStatus.Unlocked, response.LeaseStatus);
            Assert.AreEqual(LeaseState.Available, response.LeaseState);
        }

        [Test]
        [ExpectedException(typeof(ContainerNotFoundAzureException))]
        public void GetContainerProperties_NonexistentContainer_ThrowsContainerNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);

            client.GetContainerProperties(containerName);

            // expects exception
        }

        [Test]
        public void GetContainerProperties_ContainerWithMetadata_ReturnsMetadata()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName, new Dictionary<string, string>() {
                { "a", "1" },
                { "b", "2" }
            });

            var response = client.GetContainerProperties(containerName);

            Assert.IsNotNull(response.Metadata);
            Assert.AreEqual(2, response.Metadata.Count);
            Assert.IsTrue(response.Metadata.Any(kvp => kvp.Key == "a" && kvp.Value == "1"));
            Assert.IsTrue(response.Metadata.Any(kvp => kvp.Key == "b" && kvp.Value == "2"));
        }

        [Test]
        public void GetContainerProperties_FixedLeaseContainer_ReturnsLeaseDetails()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.LeaseContainer(containerName, TimeSpan.FromSeconds(30), null);

            var response = client.GetContainerProperties(containerName);

            Assert.AreEqual(LeaseStatus.Locked, response.LeaseStatus);
            Assert.AreEqual(LeaseDuration.Fixed, response.LeaseDuration);
            Assert.AreEqual(LeaseState.Leased, response.LeaseState);
        }

        [Test]
        public void GetContainerProperties_InfiniteLeaseContainer_ReturnsLeaseDetails()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            string lease =_util.LeaseContainer(containerName, null, null);
            try
            {

                var response = client.GetContainerProperties(containerName);

                Assert.AreEqual(LeaseStatus.Locked, response.LeaseStatus);
                Assert.AreEqual(LeaseDuration.Infinite, response.LeaseDuration);
                Assert.AreEqual(LeaseState.Leased, response.LeaseState);
            }
            finally
            {
               _util.ReleaseContainerLease(containerName, lease);
            }
        }

        [Test]
        public void GetContainerProperties_BreakingLeaseContainer_ReturnsLeaseDetails()
        {
            const int leaseLength = 30;
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            string lease =_util.LeaseContainer(containerName, TimeSpan.FromSeconds(leaseLength), null);
           _util.BreakContainerLease(containerName, lease, leaseLength);
            try
            {

                var response = client.GetContainerProperties(containerName);

                Assert.AreEqual(LeaseStatus.Locked, response.LeaseStatus);
                Assert.AreEqual(LeaseState.Breaking, response.LeaseState);
            }
            finally
            {
               _util.ReleaseContainerLease(containerName, lease);
            }
        }

        [Test]
        public async Task GetContainerPropertiesAsync_ValidContainer_ReturnsProperties()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            var response = await client.GetContainerPropertiesAsync(containerName);

            Assert.IsNotNull(response.Metadata);
            Assert.IsNotNullOrEmpty(response.ETag);
            Assert.AreEqual(LeaseStatus.Unlocked, response.LeaseStatus);
            Assert.AreEqual(LeaseState.Available, response.LeaseState);
        }

        [Test]
        [ExpectedException(typeof(ContainerNotFoundAzureException))]
        public async Task GetContainerPropertiesAsync_NonexistentContainer_ThrowsContainerNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);

            await client.GetContainerPropertiesAsync(containerName);

            // expects exception
        }

        [Test]
        public void GetContainerMetadata_ValidContainer_ReturnsMetadata()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName, new Dictionary<string, string>() {
                { "a", "1" },
                { "b", "2" }
            });

            var response = client.GetContainerMetadata(containerName);

            Assert.IsNotNull(response.Metadata);
            Assert.AreEqual(2, response.Metadata.Count);
            Assert.IsTrue(response.Metadata.Any(kvp => kvp.Key == "a" && kvp.Value == "1"));
            Assert.IsTrue(response.Metadata.Any(kvp => kvp.Key == "b" && kvp.Value == "2"));
        }

        [Test]
        public void GetContainerMetadata_ValidContainerWithNoMetadata_ReturnsEmptyMetadata()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName, new Dictionary<string, string>());

            var response = client.GetContainerMetadata(containerName);

            Assert.IsNotNull(response.Metadata);
            Assert.AreEqual(0, response.Metadata.Count);
        }

        [Test]
        [ExpectedException(typeof(ContainerNotFoundAzureException))]
        public void GetContainerMetadata_NonexistentContainer_ThrowsContainerNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);

            client.GetContainerMetadata(containerName);

            //expects exception
        }

        [Test]
        public async Task GetContainerMetadataAsync_ValidContainer_ReturnsMetadata()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName, new Dictionary<string, string>() {
                { "a", "1" },
                { "b", "2" }
            });

            var response = await client.GetContainerMetadataAsync(containerName);

            Assert.IsNotNull(response.Metadata);
            Assert.AreEqual(2, response.Metadata.Count);
            Assert.IsTrue(response.Metadata.Any(kvp => kvp.Key == "a" && kvp.Value == "1"));
            Assert.IsTrue(response.Metadata.Any(kvp => kvp.Key == "b" && kvp.Value == "2"));
        }

        [Test]
        [ExpectedException(typeof(ContainerNotFoundAzureException))]
        public async Task GetContainerMetadataAsync_NonexistentContainer_ThrowsContainerNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);

            await client.GetContainerMetadataAsync(containerName);

            //expects exception
        }

        [Test]
        public void SetContainerMetadata_ValidContainer_SetsMetadataOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            client.SetContainerMetadata(containerName, new Dictionary<string, string>() {
                { "a", "1"},
                { "b", "2"}
            });

            var metadata =_util.GetContainerMetadata(containerName);
            Assert.IsNotNull(metadata);
            Assert.AreEqual(2, metadata.Count);
            Assert.IsTrue(metadata.Any(kvp => kvp.Key == "a" && kvp.Value == "1"));
            Assert.IsTrue(metadata.Any(kvp => kvp.Key == "b" && kvp.Value == "2"));
        }

        [Test]
        public void SetContainerMetadata_LeasedContainerWithoutLease_SetsMetadataOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.LeaseContainer(containerName, null, null);

            client.SetContainerMetadata(containerName, new Dictionary<string, string>() {
                { "a", "1"},
                { "b", "2"}
            });

            var metadata =_util.GetContainerMetadata(containerName);
            Assert.IsNotNull(metadata);
            Assert.AreEqual(2, metadata.Count);
            Assert.IsTrue(metadata.Any(kvp => kvp.Key == "a" && kvp.Value == "1"));
            Assert.IsTrue(metadata.Any(kvp => kvp.Key == "b" && kvp.Value == "2"));
        }

        [Test]
        public void SetContainerMetadata_LeasedContainerWithLease_SetsMetadataOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            var lease =_util.LeaseContainer(containerName, null, null);

            client.SetContainerMetadata(containerName, new Dictionary<string, string>() {
                { "a", "1"},
                { "b", "2"}
            }, lease);

            var metadata =_util.GetContainerMetadata(containerName);
            Assert.IsNotNull(metadata);
            Assert.AreEqual(2, metadata.Count);
            Assert.IsTrue(metadata.Any(kvp => kvp.Key == "a" && kvp.Value == "1"));
            Assert.IsTrue(metadata.Any(kvp => kvp.Key == "b" && kvp.Value == "2"));
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithContainerOperationAzureException))]
        public void SetContainerMetadata_NonLeasedContainerWithLease_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            client.SetContainerMetadata(containerName, new Dictionary<string, string>() {
                { "a", "1"},
                { "b", "2"}
            }, FakeLeaseId);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithContainerOperationAzureException))]
        public void SetContainerMetadata_WrongLeasForLeasedContainer_ThrowsLeaseMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.LeaseContainer(containerName, null, null);

            client.SetContainerMetadata(containerName, new Dictionary<string, string>() {
                { "a", "1"},
                { "b", "2"}
            }, FakeLeaseId);

            // expects exception
        }

        [Test]
        public async Task SetContainerMetadataAsync_ValidContainer_SetsMetadataOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            await client.SetContainerMetadataAsync(containerName, new Dictionary<string, string>() {
                { "a", "1"},
                { "b", "2"}
            });

            var metadata =_util.GetContainerMetadata(containerName);
            Assert.IsNotNull(metadata);
            Assert.AreEqual(2, metadata.Count);
            Assert.IsTrue(metadata.Any(kvp => kvp.Key == "a" && kvp.Value == "1"));
            Assert.IsTrue(metadata.Any(kvp => kvp.Key == "b" && kvp.Value == "2"));
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithContainerOperationAzureException))]
        public async Task SetContainerMetadataAsync_NonLeasedContainerWithLease_ThrowsPreconditionFailureException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            await client.SetContainerMetadataAsync(containerName, new Dictionary<string, string>() {
                { "a", "1"},
                { "b", "2"}
            }, FakeLeaseId);

            // expects exception
        }

        [Test]
        public void GetContainerACL_NoAccessPolicies_ReturnsEmptyList()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            var result = client.GetContainerACL(containerName);

            Assert.IsEmpty(result.SignedIdentifiers);
        }

        [Test]
        public void GetContainerACL_HasAccessPolicies_ReturnsListConstainingThosePolicies()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            string expectedId = "abc-123";
            DateTime expectedStart = GetTruncatedUtcNow();
           _util.AddContainerAccessPolicy(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off, expectedId, expectedStart, expectedStart.AddDays(1));

            var result = client.GetContainerACL(containerName);

            Assert.IsNotEmpty(result.SignedIdentifiers);
            Assert.AreEqual("abc-123", result.SignedIdentifiers.First().Id);
            Assert.AreEqual(expectedStart, result.SignedIdentifiers.First().AccessPolicy.StartTime);
            Assert.AreEqual(expectedStart.AddDays(1), result.SignedIdentifiers.First().AccessPolicy.Expiry);
        }

        [Test]
        public void GetContainerACL_ContainerAccess_ReturnsPublicAccessSet()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.AddContainerAccessPolicy(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Container);

            var result = client.GetContainerACL(containerName);

            Assert.AreEqual(ContainerAccessType.PublicContainer, result.PublicAccess);
        }

        [Test]
        public void GetContainerACL_NoPublicAccess_ReturnsPublicAccessAsNone()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.AddContainerAccessPolicy(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off);

            var result = client.GetContainerACL(containerName);

            Assert.AreEqual(ContainerAccessType.None, result.PublicAccess);
        }

        [Test]
        [ExpectedException(typeof(ContainerNotFoundAzureException))]
        public void GetContainerACL_NonexistentQueue_ThrowsQueueNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);

            client.GetContainerACL(containerName);

            // expects exception
        }

        [Test]
        public async Task GetContainerACLAsync_HasAccessPolicies_ReturnsListConstainingThosePolicies()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            string expectedId = "abc-123";
            DateTime expectedStart = GetTruncatedUtcNow();
           _util.AddContainerAccessPolicy(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off, expectedId, expectedStart, expectedStart.AddDays(1));

            var result = await client.GetContainerACLAsync(containerName);

            Assert.IsNotEmpty(result.SignedIdentifiers);
            Assert.AreEqual("abc-123", result.SignedIdentifiers.First().Id);
            Assert.AreEqual(expectedStart, result.SignedIdentifiers.First().AccessPolicy.StartTime);
            Assert.AreEqual(expectedStart.AddDays(1), result.SignedIdentifiers.First().AccessPolicy.Expiry);
        }

        [Test]
        [ExpectedException(typeof(ContainerNotFoundAzureException))]
        public async Task GetContainerACLAsync_NonexistentQueue_ThrowsQueueNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);

            await client.GetContainerACLAsync(containerName);

            // expects exception
        }

        [Test]
        public void SetContainerACL_ReadPolicyForValidContainer_SetsPolicyAndPublicAccessOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            var expectedStartTime = GetTruncatedUtcNow();
            var expectedIdentifier = new BlobSignedIdentifier()
            {
                Id = "abc-123",
                AccessPolicy = new BlobAccessPolicy()
                {
                    StartTime = expectedStartTime,
                    Expiry = expectedStartTime.AddHours(1),
                    Permission = BlobSharedAccessPermissions.Read
                }
            };

            client.SetContainerACL(containerName, ContainerAccessType.PublicContainer, new List<BlobSignedIdentifier>() { expectedIdentifier });

            var actual =_util.GetContainerPermissions(containerName);
            Assert.AreEqual(1, actual.SharedAccessPolicies.Count);
           _util.AssertIdentifierInSharedAccessPolicies(actual.SharedAccessPolicies, expectedIdentifier, Microsoft.WindowsAzure.Storage.Blob.SharedAccessBlobPermissions.Read);
        }

        [Test]
        public void SetContainerACL_AllPolicyForValidContainer_SetsPolicyAndPublicAccessOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            var expectedStartTime = GetTruncatedUtcNow();
            var expectedIdentifier = new BlobSignedIdentifier()
            {
                Id = "abc-123",
                AccessPolicy = new BlobAccessPolicy()
                {
                    StartTime = expectedStartTime,
                    Expiry = expectedStartTime.AddHours(1),
                    Permission = BlobSharedAccessPermissions.Read | BlobSharedAccessPermissions.Write | BlobSharedAccessPermissions.Delete | BlobSharedAccessPermissions.List
                }
            };

            client.SetContainerACL(containerName, ContainerAccessType.PublicContainer, new List<BlobSignedIdentifier>() { expectedIdentifier });

            var actual =_util.GetContainerPermissions(containerName);
            Assert.AreEqual(1, actual.SharedAccessPolicies.Count);
           _util.AssertIdentifierInSharedAccessPolicies(actual.SharedAccessPolicies, expectedIdentifier, Microsoft.WindowsAzure.Storage.Blob.SharedAccessBlobPermissions.Read | Microsoft.WindowsAzure.Storage.Blob.SharedAccessBlobPermissions.Write | Microsoft.WindowsAzure.Storage.Blob.SharedAccessBlobPermissions.List | Microsoft.WindowsAzure.Storage.Blob.SharedAccessBlobPermissions.Delete);
        }

        [Test]
        public void SetContainerACL_PublicAccessAndNoPolicyForValidContainer_SetsPublicAccessOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            client.SetContainerACL(containerName, ContainerAccessType.PublicBlob, new List<BlobSignedIdentifier>());

            var actual =_util.GetContainerPermissions(containerName);
            Assert.AreEqual(0, actual.SharedAccessPolicies.Count);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Blob, actual.PublicAccess);
        }

        [Test]
        public void SetContainerACL_NoPublicAccessAndPolicyForValidContainer_ClearsPublicAccessOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.AddContainerAccessPolicy(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Container);

            client.SetContainerACL(containerName, ContainerAccessType.None, new List<BlobSignedIdentifier>());

            var actual =_util.GetContainerPermissions(containerName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off, actual.PublicAccess);
        }

        [Test]
        [ExpectedException(typeof(ContainerNotFoundAzureException))]
        public void SetContainerACL_InvalidContainer_ThrowsContainerNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);

            client.SetContainerACL(containerName, ContainerAccessType.None, new List<BlobSignedIdentifier>());

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithContainerOperationAzureException))]
        public void SetContainerACL_LeaseForNonLeasedContainer_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            client.SetContainerACL(containerName, ContainerAccessType.None, new List<BlobSignedIdentifier>(), FakeLeaseId);

            var actual =_util.GetContainerPermissions(containerName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off, actual.PublicAccess);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithContainerOperationAzureException))]
        public void SetContainerACL_WrongLeaseForLeasedContainer_ThrowsLeaseMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.LeaseContainer(containerName, null, null);

            client.SetContainerACL(containerName, ContainerAccessType.None, new List<BlobSignedIdentifier>(), FakeLeaseId);

            var actual =_util.GetContainerPermissions(containerName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off, actual.PublicAccess);
        }

        [Test]
        public void SetContainerACL_LeaseForLeasedContainer_SetsPolicySuccesfully()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            string leaseId =_util.LeaseContainer(containerName, null, null);
            var expectedStartTime = GetTruncatedUtcNow();
            var expectedIdentifier = new BlobSignedIdentifier()
            {
                Id = "abc-123",
                AccessPolicy = new BlobAccessPolicy()
                {
                    StartTime = expectedStartTime,
                    Expiry = expectedStartTime.AddHours(1),
                    Permission = BlobSharedAccessPermissions.Read | BlobSharedAccessPermissions.Write | BlobSharedAccessPermissions.Delete | BlobSharedAccessPermissions.List
                }
            };

            client.SetContainerACL(containerName, ContainerAccessType.PublicContainer, new List<BlobSignedIdentifier>() { expectedIdentifier }, leaseId);

            var actual =_util.GetContainerPermissions(containerName);
            Assert.AreEqual(1, actual.SharedAccessPolicies.Count);
        }

        [Test]
        public void SetContainerACL_NoLeaseForLeasedContainer_SetsPolicySuccesfully()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.LeaseContainer(containerName, null, null);
            var expectedStartTime = GetTruncatedUtcNow();
            var expectedIdentifier = new BlobSignedIdentifier()
            {
                Id = "abc-123",
                AccessPolicy = new BlobAccessPolicy()
                {
                    StartTime = expectedStartTime,
                    Expiry = expectedStartTime.AddHours(1),
                    Permission = BlobSharedAccessPermissions.Read | BlobSharedAccessPermissions.Write | BlobSharedAccessPermissions.Delete | BlobSharedAccessPermissions.List
                }
            };

            client.SetContainerACL(containerName, ContainerAccessType.PublicContainer, new List<BlobSignedIdentifier>() { expectedIdentifier });

            var actual =_util.GetContainerPermissions(containerName);
            Assert.AreEqual(1, actual.SharedAccessPolicies.Count);
        }

        [Test]
        public async Task SetContainerACLAsync_ReadPolicyForValidContainer_SetsPolicyAndPublicAccessOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            var expectedStartTime = GetTruncatedUtcNow();
            var expectedIdentifier = new BlobSignedIdentifier()
            {
                Id = "abc-123",
                AccessPolicy = new BlobAccessPolicy()
                {
                    StartTime = expectedStartTime,
                    Expiry = expectedStartTime.AddHours(1),
                    Permission = BlobSharedAccessPermissions.Read
                }
            };

            await client.SetContainerACLAsync(containerName, ContainerAccessType.PublicContainer, new List<BlobSignedIdentifier>() { expectedIdentifier });

            var actual =_util.GetContainerPermissions(containerName);
            Assert.AreEqual(1, actual.SharedAccessPolicies.Count);
           _util.AssertIdentifierInSharedAccessPolicies(actual.SharedAccessPolicies, expectedIdentifier, Microsoft.WindowsAzure.Storage.Blob.SharedAccessBlobPermissions.Read);
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithContainerOperationAzureException))]
        public async Task SetContainerACLAsync_LeaseForNonLeasedContainer_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            await client.SetContainerACLAsync(containerName, ContainerAccessType.None, new List<BlobSignedIdentifier>(), FakeLeaseId);

            var actual =_util.GetContainerPermissions(containerName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off, actual.PublicAccess);
        }

        [Test]
        public void DeleteContainer_ValidContainer_DeletesTheContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            client.DeleteContainer(containerName);

           _util.AssertContainerDoesNotExist(containerName);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMissingAzureException))]
        public void DeleteContainer_NoLeaseForLeasedContainer_ThrowsLeaseIdMissingException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.LeaseContainer(containerName, null, null);

            client.DeleteContainer(containerName);

           _util.AssertContainerDoesNotExist(containerName);
        }

        [Test]
        public void DeleteContainer_LeaseForLeasedContainer_DeletesContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            var leaseId =_util.LeaseContainer(containerName, null, null);

            client.DeleteContainer(containerName, leaseId);

           _util.AssertContainerDoesNotExist(containerName);
        }

        [Test]
        [ExpectedException(typeof(ContainerNotFoundAzureException))]
        public void DeleteContainer_NonExistentContainer_ThrowsContainerNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);

            client.DeleteContainer(containerName);

           _util.AssertContainerDoesNotExist(containerName);
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithContainerOperationAzureException))]
        public void DeleteContainer_LeaseForNonLeasedContainer_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            client.DeleteContainer(containerName, FakeLeaseId);

           _util.AssertContainerDoesNotExist(containerName);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithContainerOperationAzureException))]
        public void DeleteContainer_WrongLeaseForLeasedContainer_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.LeaseContainer(containerName, null, null);

            client.DeleteContainer(containerName, FakeLeaseId);

           _util.AssertContainerDoesNotExist(containerName);
        }

        [Test]
        public async Task DeleteContainerAsync_ValidContainer_DeletesTheContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            await client.DeleteContainerAsync(containerName);

           _util.AssertContainerDoesNotExist(containerName);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithContainerOperationAzureException))]
        public async Task DeleteContainerAsync_WrongLeaseForLeasedContainer_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.LeaseContainer(containerName, null, null);

            await client.DeleteContainerAsync(containerName, FakeLeaseId);

           _util.AssertContainerDoesNotExist(containerName);
        }

        [Test]
        public void LeaseContainerAcquire_AcquireLeaseForValidContainer_AcquiresLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            var response = client.LeaseContainerAcquire(containerName, 30);

           _util.AssertContainerIsLeased(containerName, response.LeaseId);
           _util.RegisterContainerForCleanup(containerName, response.LeaseId);
        }

        [Test]
        public void LeaseContainerAcquire_AcquireInfiniteLeaseForValidContainer_AcquiresLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            var response = client.LeaseContainerAcquire(containerName, -1);

           _util.AssertContainerIsLeased(containerName, response.LeaseId);
           _util.RegisterContainerForCleanup(containerName, response.LeaseId);
        }

        [Test]
        public void LeaseContainerAcquire_AcquireSpecificLeaseIdForValidContainer_AcquiresLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            string expectedId = Guid.NewGuid().ToString();

            var response = client.LeaseContainerAcquire(containerName, 30, expectedId);

            Assert.AreEqual(expectedId, response.LeaseId);
           _util.AssertContainerIsLeased(containerName, response.LeaseId);
           _util.RegisterContainerForCleanup(containerName, response.LeaseId);
        }

        [Test]
        [ExpectedException(typeof(ContainerNotFoundAzureException))]
        public void LeaseContainerAcquire_InvalidContainer_ThrowsContainerNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);

            client.LeaseContainerAcquire(containerName, 30);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(LeaseAlreadyPresentAzureException))]
        public void LeaseContainerAcquire_AlreadyLeasedContainer_ThrowsAlreadyPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.LeaseContainer(containerName, null, null);

            client.LeaseContainerAcquire(containerName, 30);

            // expects exception
        }

        [Test]
        public async Task LeaseContainerAcquireAsync_AcquireLeaseForValidContainer_AcquiresLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            var response = await client.LeaseContainerAcquireAsync(containerName, 30);

           _util.AssertContainerIsLeased(containerName, response.LeaseId);
           _util.RegisterContainerForCleanup(containerName, response.LeaseId);
        }

        [Test]
        [ExpectedException(typeof(LeaseAlreadyPresentAzureException))]
        public async Task LeaseContainerAcquireAsync_AlreadyLeasedContainer_ThrowsAlreadyPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.LeaseContainer(containerName, null, null);

            await client.LeaseContainerAcquireAsync(containerName, 30);

            // expects exception
        }

        [Test]
        public void LeaseContainerRenew_LeasedContainer_RenewsActiveLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            var leaseId =_util.LeaseContainer(containerName, TimeSpan.FromSeconds(30), null);

            client.LeaseContainerRenew(containerName, leaseId);

            // how do I test this?
            // it didn't blow up and it's still leased?
           _util.AssertContainerIsLeased(containerName, leaseId);
        }

        [Test]
        [Ignore("This test is long by design because we have to wait for the lease to release before we attempt to renew")]
        public void LeaseContainerRenew_RecentlyLeasedContainer_RenewsLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            var leaseId =_util.LeaseContainer(containerName, TimeSpan.FromSeconds(15), null);
            Thread.Sleep(16);

            client.LeaseContainerRenew(containerName, leaseId);

            // how do I test this?
            // it didn't blow up and it's still leased?
           _util.AssertContainerIsLeased(containerName, leaseId);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithLeaseOperationAzureException))]
        public void LeaseContainerRenew_NonLeasedContainer_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            client.LeaseContainerRenew(containerName, FakeLeaseId);

            // expects exception
        }

        [Test]
        public async Task LeaseContainerRenewAsync_LeasedContainer_RenewsActiveLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            var leaseId =_util.LeaseContainer(containerName, TimeSpan.FromSeconds(30), null);

            await client.LeaseContainerRenewAsync(containerName, leaseId);

            // how do I test this?
            // it didn't blow up and it's still leased?
           _util.AssertContainerIsLeased(containerName, leaseId);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithLeaseOperationAzureException))]
        public async Task LeaseContainerRenewAsync_NonLeasedContainer_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            await client.LeaseContainerRenewAsync(containerName, FakeLeaseId);

            // expects exception
        }

        [Test]
        public void LeaseContainerChange_LeasedContainerToNewLeaseId_ChangesToMatchNewLeaseId()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            string leaseId =_util.LeaseContainer(containerName, null, null);
            string expectedLeaseId = Guid.NewGuid().ToString();

            var response = client.LeaseContainerChange(containerName, leaseId, expectedLeaseId);

            Assert.AreEqual(expectedLeaseId, response.LeaseId);
           _util.AssertContainerIsLeased(containerName, expectedLeaseId);
           _util.RegisterContainerForCleanup(containerName, expectedLeaseId);
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithLeaseOperationAzureException))]
        public void LeaseContainerChange_NonLeasedContainer_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            client.LeaseContainerChange(containerName, FakeLeaseId, expectedLeaseId);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(ContainerNotFoundAzureException))]
        public void LeaseContainerChange_NonexistentContainer_ThrowsContainerNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var expectedLeaseId = Guid.NewGuid().ToString();

            client.LeaseContainerChange(containerName, FakeLeaseId, expectedLeaseId);

            // expects exception
        }

        [Test]
        public async Task LeaseContainerChangeAsync_LeasedContainerToNewLeaseId_ChangesToMatchNewLeaseId()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            string leaseId =_util.LeaseContainer(containerName, null, null);
            string expectedLeaseId = Guid.NewGuid().ToString();

            var response = await client.LeaseContainerChangeAsync(containerName, leaseId, expectedLeaseId);

            Assert.AreEqual(expectedLeaseId, response.LeaseId);
           _util.AssertContainerIsLeased(containerName, expectedLeaseId);
           _util.RegisterContainerForCleanup(containerName, expectedLeaseId);
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithLeaseOperationAzureException))]
        public async Task LeaseContainerChangeAsync_NonLeasedContainer_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            await client.LeaseContainerChangeAsync(containerName, FakeLeaseId, expectedLeaseId);

            // expects exception
        }

        [Test]
        public void LeaseContainerRelease_LeasedContainer_ReleasesLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            var leaseId =_util.LeaseContainer(containerName, null, null);

            client.LeaseContainerRelease(containerName, leaseId);

           _util.AssertContainerIsNotLeased(containerName);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithLeaseOperationAzureException))]
        public void LeaseContainerRelease_NonLeasedContainer_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            client.LeaseContainerRelease(containerName, FakeLeaseId);

            // expects exception
        }


        [Test]
        public async Task LeaseContainerReleaseAsync_LeasedContainer_ReleasesLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            var leaseId =_util.LeaseContainer(containerName, null, null);

            await client.LeaseContainerReleaseAsync(containerName, leaseId);

           _util.AssertContainerIsNotLeased(containerName);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithLeaseOperationAzureException))]
        public async Task LeaseContainerReleaseAsync_NonLeasedContainer_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            await client.LeaseContainerReleaseAsync(containerName, FakeLeaseId);

            // expects exception
        }

        [Test]
        public void LeaseContainerBreak_LeasedContainer_BreaksLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            var leaseId =_util.LeaseContainer(containerName, null, null);

            client.LeaseContainerBreak(containerName, leaseId, 0);

            var leaseState =_util.GetContainerLeaseState(containerName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.LeaseState.Broken, leaseState);
        }

        [Test]
        public void LeaseContainerBreak_LeasedContainerWithLongBreakPeriod_SetLeaseToBreakinge()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            var leaseId =_util.LeaseContainer(containerName, null, null);

            client.LeaseContainerBreak(containerName, leaseId, 60);

            var leaseState =_util.GetContainerLeaseState(containerName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.LeaseState.Breaking, leaseState);
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithLeaseOperationAzureException))]
        public void LeaseContainerBreak_NonLeasedContainer_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            client.LeaseContainerBreak(containerName, FakeLeaseId, 0);

            // expects exception
        }

        [Test]
        public async Task LeaseContainerBreakAsync_LeasedContainerWithLongBreakPeriod_SetLeaseToBreakinge()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
            var leaseId =_util.LeaseContainer(containerName, null, null);

            await client.LeaseContainerBreakAsync(containerName, leaseId, 60);

            var leaseState =_util.GetContainerLeaseState(containerName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.LeaseState.Breaking, leaseState);
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithLeaseOperationAzureException))]
        public async Task LeaseContainerBreakAsync_NonLeasedContainer_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            await client.LeaseContainerBreakAsync(containerName, FakeLeaseId, 0);

            // expects exception
        }

        [Test]
        public void ListBlobs_EmptyContainer_ReturnsEmptyList()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);

            var result = client.ListBlobs(containerName);

            Assert.IsEmpty(result.BlobList);
        }

        [Test]
        public void ListBlobs_PopulatedContainer_ReturnsExpectedBlobsInList()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.CreateBlockBlob(containerName, "blob/UnitTest/SampleA");
           _util.CreateBlockBlob(containerName, "blob/UnitTest/SampleB");

            var result = client.ListBlobs(containerName);

            Assert.AreEqual(2, result.BlobList.Count);
            Assert.IsTrue(result.BlobList.Any(b => b.Name == "blob/UnitTest/SampleA"));
            Assert.IsTrue(result.BlobList.Any(b => b.Name == "blob/UnitTest/SampleB"));
        }

        [Test]
        public void ListBlobs_PrefixSupplied_ReturnsOnlyBlobsMatchingThatPrefix()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.CreateBlockBlob(containerName, "blob/UnitTest/SampleA");
           _util.CreateBlockBlob(containerName, "blob/UnitTest/SampleB");
           _util.CreateBlockBlob(containerName, "SomethingElse.txt");

            var result = client.ListBlobs(containerName, prefix: "blob");

            Assert.AreEqual(2, result.BlobList.Count);
            Assert.IsTrue(result.BlobList.Any(b => b.Name == "blob/UnitTest/SampleA"));
            Assert.IsTrue(result.BlobList.Any(b => b.Name == "blob/UnitTest/SampleB"));
        }

        [Test]
        public void ListBlobs_PrefixAndDelimiterSupplied_ReturnsOnlyBlobsMatchingThatPrefixWithNamesTruncatedAtDelimiter()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.CreateBlockBlob(containerName, "blob/UnitTest/SampleA");
           _util.CreateBlockBlob(containerName, "blob/UnitTest/SampleB");
           _util.CreateBlockBlob(containerName, "SomethingElse.txt");
            string expectedPrefix = "blob/UnitTest/";

            var result = client.ListBlobs(containerName, prefix: expectedPrefix, delimiter: "/");

            Assert.AreEqual(2, result.BlobList.Count);
            Assert.AreEqual(expectedPrefix, result.Prefix);
            Assert.IsTrue(result.BlobList.Any(b => b.Name == "blob/UnitTest/SampleA"));
            Assert.IsTrue(result.BlobList.Any(b => b.Name == "blob/UnitTest/SampleB"));
        }

        [Test]
        public void ListBlobs_SmallerMaxResultsThanBlobCount_ReturnsResultsAndContinuationMarker()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.CreateBlockBlob(containerName, "blob1");
           _util.CreateBlockBlob(containerName, "blob2");
           _util.CreateBlockBlob(containerName, "blob3");

            var result = client.ListBlobs(containerName, maxResults: 2);

            Assert.AreEqual(2, result.BlobList.Count);
            Assert.AreEqual(2, result.MaxResults);
            Assert.IsNotNullOrEmpty(result.NextMarker);
        }

        [Test]
        public void ListBlobs_MarkerSuppliedForLongerList_ReturnsNextSetofResults()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.CreateBlockBlob(containerName, "blob1");
           _util.CreateBlockBlob(containerName, "blob2");
           _util.CreateBlockBlob(containerName, "blob3");

            var result = client.ListBlobs(containerName, maxResults: 2);
            var nextMarker = result.NextMarker;
            var remainingResult = client.ListBlobs(containerName, marker: nextMarker);

            Assert.AreEqual(1, remainingResult.BlobList.Count);
            Assert.IsNotNullOrEmpty(remainingResult.Marker);
        }

        [Test]
        public void ListBlobs_IncludeMetadata_ReturnsMetadataInResults()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.CreateBlockBlob(containerName, "blob1", new Dictionary<string, string>() {
                { "a", "1"},
                { "b", "2"}
            });

            var result = client.ListBlobs(containerName, include: ListBlobsInclude.Metadata);

            Assert.AreEqual("blob1", result.BlobList[0].Name, "The list blob results did not include the test blob we setup with metadata as the first entry");
            Assert.IsNotEmpty(result.BlobList[0].Metadata);
            Assert.AreEqual(2, result.BlobList[0].Metadata.Count);
            Assert.IsTrue(result.BlobList[0].Metadata.Any(kvp => kvp.Key == "a" && kvp.Value == "1"));
            Assert.IsTrue(result.BlobList[0].Metadata.Any(kvp => kvp.Key == "b" && kvp.Value == "2"));
        }

        [Test]
        public void ListBlobs_IncludeCopy_ReturnsCopyStatus()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.CreateBlockBlob(containerName, "blob1");
           _util.CopyBlob(containerName, "blob1", "blob1copy");

            var result = client.ListBlobs(containerName, include: ListBlobsInclude.Copy);

            var copiedBlob = result.BlobList.Where(b => b.Name == "blob1copy").FirstOrDefault();
            Assert.IsNotNull(copiedBlob);
            Assert.IsNotEmpty(copiedBlob.Properties.CopyId);
            Assert.IsNotNull(copiedBlob.Properties.CopySource);
            Assert.IsNotEmpty(copiedBlob.Properties.CopyProgress);
            Assert.IsNotNull(copiedBlob.Properties.CopyStatus);
        }

        [Test]
        public void ListBlobs_IncludeSnapshots_ReturnsSnapshotDetails()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.CreateBlockBlob(containerName, "blob1");
           _util.SnapshotBlob(containerName, "blob1");

            var result = client.ListBlobs(containerName, include: ListBlobsInclude.Snapshots);

            Assert.AreEqual(2, result.BlobList.Count);
            Assert.IsTrue(result.BlobList.Any(b => b.Snapshot.HasValue));
            Assert.IsTrue(result.BlobList.Any(b => !b.Snapshot.HasValue));
        }

        [Test]
        public void ListBlobs_IncludeUncommittedBlobs_ReturnsUncommittedBlobs()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.CreateBlobUncommitted(containerName, "blob1");

            var result = client.ListBlobs(containerName, include: ListBlobsInclude.UncomittedBlobs);

            Assert.AreEqual(1, result.BlobList.Count);
        }

        [Test]
        public void ListBlobs_BlockAndPageBlobs_ReturnsBothTypes()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.CreateBlockBlob(containerName, "blob1");
           _util.CreatePageBlob(containerName, "blob2");

            var result = client.ListBlobs(containerName);

            Assert.AreEqual(2, result.BlobList.Count);
            Assert.IsTrue(result.BlobList.Any(b => b.Properties.BlobType == Communications.Common.BlobType.Block));
            Assert.IsTrue(result.BlobList.Any(b => b.Properties.BlobType == Communications.Common.BlobType.Page));
        }

        [Test]
        public async Task ListBlobsAsync_PopulatedContainer_ReturnsExpectedBlobsInList()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
           _util.CreateContainer(containerName);
           _util.CreateBlockBlob(containerName, "blob/UnitTest/SampleA");
           _util.CreateBlockBlob(containerName, "blob/UnitTest/SampleB");

            var result = await client.ListBlobsAsync(containerName);

            Assert.AreEqual(2, result.BlobList.Count);
            Assert.IsTrue(result.BlobList.Any(b => b.Name == "blob/UnitTest/SampleA"));
            Assert.IsTrue(result.BlobList.Any(b => b.Name == "blob/UnitTest/SampleB"));
        }

        #endregion

    }
}
