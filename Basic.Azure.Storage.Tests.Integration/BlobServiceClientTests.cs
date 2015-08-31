using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Microsoft.WindowsAzure.Storage;
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

namespace Basic.Azure.Storage.Tests.Integration
{
    [TestFixture]
    public class BlobServiceClientTests
    {

        private readonly StorageAccountSettings _accountSettings = new LocalEmulatorAccountSettings();
        private readonly CloudStorageAccount _storageAccount = CloudStorageAccount.Parse("UseDevelopmentStorage=true");

        private readonly Dictionary<string, string> _containersToCleanUp = new Dictionary<string, string>();

        private string GenerateSampleContainerName()
        {
            var name = "unit-test-" + Guid.NewGuid().ToString().ToLower();
            RegisterContainerForCleanup(name, null);
            return name;
        }

        private string GenerateSampleBlobName()
        {
            return String.Format("unit-test-{0}", Guid.NewGuid());
        }

        private void RegisterContainerForCleanup(string containerName, string leaseId)
        {
            _containersToCleanUp[containerName] = leaseId;
        }

        public string FakeLeaseId { get { return "a28cf439-8776-4653-9ce8-4e3df49b4a72"; } }

        [TestFixtureTearDown]
        public void TestFixtureTeardown()
        {
            //let's clean up!
            var client = _storageAccount.CreateCloudBlobClient();
            foreach (var containerPair in _containersToCleanUp)
            {
                var container = client.GetContainerReference(containerPair.Key);
                if (!string.IsNullOrEmpty(containerPair.Value))
                {
                    try
                    {
                        container.ReleaseLease(new AccessCondition() { LeaseId = containerPair.Value });
                    }
                    catch { }
                }
                container.DeleteIfExists();
            }
        }

        #region Account Operations

        #endregion

        #region Container Operations Tests

        [Test]
        public void CreateContainer_ValidArguments_CreatesContainerWithSpecificName()
        {
            var containerName = GenerateSampleContainerName();
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.None);

            AssertContainerExists(containerName);
        }

        [Test]
        public void CreateContainer_ValidArgumentsWithPublicContainerAccess_CreatesContainerWithContainerAccess()
        {
            var containerName = GenerateSampleContainerName();
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.PublicContainer);

            AssertContainerAccess(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Container);
        }

        [Test]
        public void CreateContainer_ValidArgumentsWithPublicBlobAccess_CreatesContainerWithBlobAccess()
        {
            var containerName = GenerateSampleContainerName();
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.PublicBlob);

            AssertContainerAccess(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Blob);
        }

        [Test]
        public void CreateContainer_ValidArgumentsWithNoPublicAccess_CreatesContainerWithNoPublicAccess()
        {
            var containerName = GenerateSampleContainerName();
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.None);

            AssertContainerAccess(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off);
        }

        [Test]
        [ExpectedException(typeof(ContainerAlreadyExistsAzureException))]
        public void CreateContainer_AlreadyExists_ThrowsContainerAlreadyExistsException()
        {
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.None);

            // expects exception
        }

        [Test]
        public void CreateContainer_ValidArguments_ReturnsContainerCreationResponse()
        {
            var containerName = GenerateSampleContainerName();
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            var response = client.CreateContainer(containerName, ContainerAccessType.None);

            Assert.IsTrue(response.Date > DateTime.Now.AddSeconds(-5), String.Format("Response Date was set to {0}", response.Date));
            Assert.IsTrue(response.LastModified > DateTime.Now.AddSeconds(-5), String.Format("Response LastModified was set to {0}", response.LastModified));
            Assert.IsFalse(string.IsNullOrWhiteSpace(response.ETag), "Response ETag is not set");

        }

        [Test]
        public async Task CreateContainerAsync_ValidArguments_CreatesContainerWithSpecificName()
        {
            var containerName = GenerateSampleContainerName();
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            await client.CreateContainerAsync(containerName, ContainerAccessType.None);

            AssertContainerExists(containerName);
        }

        [Test]
        [ExpectedException(typeof(ContainerAlreadyExistsAzureException))]
        public async Task CreateContainerAsync_AlreadyExists_ThrowsContainerAlreadyExistsException()
        {
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            await client.CreateContainerAsync(containerName, ContainerAccessType.None);

            // expects exception
        }

        [Test]
        public void GetContainerProperties_ValidContainer_ReturnsProperties()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

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
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();

            var response = client.GetContainerProperties(containerName);

            // expects exception
        }

        [Test]
        public void GetContainerProperties_ContainerWithMetadata_ReturnsMetadata()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName, new Dictionary<string, string>() { 
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
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            string lease = LeaseContainer(containerName, TimeSpan.FromSeconds(30), null);

            var response = client.GetContainerProperties(containerName);

            Assert.AreEqual(LeaseStatus.Locked, response.LeaseStatus);
            Assert.AreEqual(LeaseDuration.Fixed, response.LeaseDuration);
            Assert.AreEqual(LeaseState.Leased, response.LeaseState);
        }

        [Test]
        public void GetContainerProperties_InfiniteLeaseContainer_ReturnsLeaseDetails()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            string lease = LeaseContainer(containerName, null, null);
            try
            {

                var response = client.GetContainerProperties(containerName);

                Assert.AreEqual(LeaseStatus.Locked, response.LeaseStatus);
                Assert.AreEqual(LeaseDuration.Infinite, response.LeaseDuration);
                Assert.AreEqual(LeaseState.Leased, response.LeaseState);
            }
            finally
            {
                ReleaseContainerLease(containerName, lease);
            }
        }

        [Test]
        public void GetContainerProperties_BreakingLeaseContainer_ReturnsLeaseDetails()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            string lease = LeaseContainer(containerName, TimeSpan.FromSeconds(30), null);
            BreakContainerLease(containerName, lease);
            try
            {

                var response = client.GetContainerProperties(containerName);

                Assert.AreEqual(LeaseStatus.Locked, response.LeaseStatus);
                Assert.AreEqual(LeaseState.Breaking, response.LeaseState);
            }
            finally
            {
                ReleaseContainerLease(containerName, lease);
            }
        }

        [Test]
        public async Task GetContainerPropertiesAsync_ValidContainer_ReturnsProperties()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

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
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();

            var response = await client.GetContainerPropertiesAsync(containerName);

            // expects exception
        }

        [Test]
        public void GetContainerMetadata_ValidContainer_ReturnsMetadata()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName, new Dictionary<string, string>() { 
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
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName, new Dictionary<string, string>() { });

            var response = client.GetContainerMetadata(containerName);

            Assert.IsNotNull(response.Metadata);
            Assert.AreEqual(0, response.Metadata.Count);
        }

        [Test]
        [ExpectedException(typeof(ContainerNotFoundAzureException))]
        public void GetContainerMetadata_NonexistentContainer_ThrowsContainerNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();

            client.GetContainerMetadata(containerName);

            //expects exception
        }

        [Test]
        public async Task GetContainerMetadataAsync_ValidContainer_ReturnsMetadata()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName, new Dictionary<string, string>() { 
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
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();

            await client.GetContainerMetadataAsync(containerName);

            //expects exception
        }

        [Test]
        public void SetContainerMetadata_ValidContainer_SetsMetadataOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            client.SetContainerMetadata(containerName, new Dictionary<string, string>() { 
                { "a", "1"},
                { "b", "2"}
            });

            var metadata = GetContainerMetadata(containerName);
            Assert.IsNotNull(metadata);
            Assert.AreEqual(2, metadata.Count);
            Assert.IsTrue(metadata.Any(kvp => kvp.Key == "a" && kvp.Value == "1"));
            Assert.IsTrue(metadata.Any(kvp => kvp.Key == "b" && kvp.Value == "2"));
        }

        [Test]
        public void SetContainerMetadata_LeasedContainerWithoutLease_SetsMetadataOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            LeaseContainer(containerName, null, null);

            client.SetContainerMetadata(containerName, new Dictionary<string, string>() { 
                { "a", "1"},
                { "b", "2"}
            });

            var metadata = GetContainerMetadata(containerName);
            Assert.IsNotNull(metadata);
            Assert.AreEqual(2, metadata.Count);
            Assert.IsTrue(metadata.Any(kvp => kvp.Key == "a" && kvp.Value == "1"));
            Assert.IsTrue(metadata.Any(kvp => kvp.Key == "b" && kvp.Value == "2"));
        }

        [Test]
        public void SetContainerMetadata_LeasedContainerWithLease_SetsMetadataOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            var lease = LeaseContainer(containerName, null, null);

            client.SetContainerMetadata(containerName, new Dictionary<string, string>() { 
                { "a", "1"},
                { "b", "2"}
            }, lease);

            var metadata = GetContainerMetadata(containerName);
            Assert.IsNotNull(metadata);
            Assert.AreEqual(2, metadata.Count);
            Assert.IsTrue(metadata.Any(kvp => kvp.Key == "a" && kvp.Value == "1"));
            Assert.IsTrue(metadata.Any(kvp => kvp.Key == "b" && kvp.Value == "2"));
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithContainerOperationAzureException))]
        public void SetContainerMetadata_NonLeasedContainerWithLease_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

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
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            LeaseContainer(containerName, null, null);

            client.SetContainerMetadata(containerName, new Dictionary<string, string>() { 
                { "a", "1"},
                { "b", "2"}
            }, FakeLeaseId);

            // expects exception
        }

        [Test]
        public async Task SetContainerMetadataAsync_ValidContainer_SetsMetadataOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            await client.SetContainerMetadataAsync(containerName, new Dictionary<string, string>() { 
                { "a", "1"},
                { "b", "2"}
            });

            var metadata = GetContainerMetadata(containerName);
            Assert.IsNotNull(metadata);
            Assert.AreEqual(2, metadata.Count);
            Assert.IsTrue(metadata.Any(kvp => kvp.Key == "a" && kvp.Value == "1"));
            Assert.IsTrue(metadata.Any(kvp => kvp.Key == "b" && kvp.Value == "2"));
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithContainerOperationAzureException))]
        public async Task SetContainerMetadataAsync_NonLeasedContainerWithLease_ThrowsPreconditionFailureException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            await client.SetContainerMetadataAsync(containerName, new Dictionary<string, string>() { 
                { "a", "1"},
                { "b", "2"}
            }, FakeLeaseId);

            // expects exception
        }

        [Test]
        public void GetContainerACL_NoAccessPolicies_ReturnsEmptyList()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            var result = client.GetContainerACL(containerName);

            Assert.IsEmpty(result.SignedIdentifiers);
        }

        [Test]
        public void GetContainerACL_HasAccessPolicies_ReturnsListConstainingThosePolicies()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            string expectedId = "abc-123";
            DateTime expectedStart = GetTruncatedUtcNow();
            AddContainerAccessPolicy(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off, expectedId, expectedStart, expectedStart.AddDays(1));

            var result = client.GetContainerACL(containerName);

            Assert.IsNotEmpty(result.SignedIdentifiers);
            Assert.AreEqual("abc-123", result.SignedIdentifiers.First().Id);
            Assert.AreEqual(expectedStart, result.SignedIdentifiers.First().AccessPolicy.StartTime);
            Assert.AreEqual(expectedStart.AddDays(1), result.SignedIdentifiers.First().AccessPolicy.Expiry);
        }

        [Test]
        public void GetContainerACL_ContainerAccess_ReturnsPublicAccessSet()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            AddContainerAccessPolicy(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Container);

            var result = client.GetContainerACL(containerName);

            Assert.AreEqual(ContainerAccessType.PublicContainer, result.PublicAccess);
        }

        [Test]
        public void GetContainerACL_NoPublicAccess_ReturnsPublicAccessAsNone()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            AddContainerAccessPolicy(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off);

            var result = client.GetContainerACL(containerName);

            Assert.AreEqual(ContainerAccessType.None, result.PublicAccess);
        }

        [Test]
        [ExpectedException(typeof(ContainerNotFoundAzureException))]
        public void GetContainerACL_NonexistentQueue_ThrowsQueueNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();

            var result = client.GetContainerACL(containerName);

            // expects exception
        }

        [Test]
        public async Task GetContainerACLAsync_HasAccessPolicies_ReturnsListConstainingThosePolicies()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            string expectedId = "abc-123";
            DateTime expectedStart = GetTruncatedUtcNow();
            AddContainerAccessPolicy(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off, expectedId, expectedStart, expectedStart.AddDays(1));

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
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();

            var result = await client.GetContainerACLAsync(containerName);

            // expects exception
        }

        [Test]
        public void SetContainerACL_ReadPolicyForValidContainer_SetsPolicyAndPublicAccessOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
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

            var actual = GetContainerPermissions(containerName);
            Assert.AreEqual(1, actual.SharedAccessPolicies.Count);
            AssertIdentifierInSharedAccessPolicies(actual.SharedAccessPolicies, expectedIdentifier, Microsoft.WindowsAzure.Storage.Blob.SharedAccessBlobPermissions.Read);
        }

        [Test]
        public void SetContainerACL_AllPolicyForValidContainer_SetsPolicyAndPublicAccessOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
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

            var actual = GetContainerPermissions(containerName);
            Assert.AreEqual(1, actual.SharedAccessPolicies.Count);
            AssertIdentifierInSharedAccessPolicies(actual.SharedAccessPolicies, expectedIdentifier, Microsoft.WindowsAzure.Storage.Blob.SharedAccessBlobPermissions.Read | Microsoft.WindowsAzure.Storage.Blob.SharedAccessBlobPermissions.Write | Microsoft.WindowsAzure.Storage.Blob.SharedAccessBlobPermissions.List | Microsoft.WindowsAzure.Storage.Blob.SharedAccessBlobPermissions.Delete);
        }

        [Test]
        public void SetContainerACL_PublicAccessAndNoPolicyForValidContainer_SetsPublicAccessOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            client.SetContainerACL(containerName, ContainerAccessType.PublicBlob, new List<BlobSignedIdentifier>() { });

            var actual = GetContainerPermissions(containerName);
            Assert.AreEqual(0, actual.SharedAccessPolicies.Count);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Blob, actual.PublicAccess);
        }

        [Test]
        public void SetContainerACL_NoPublicAccessAndPolicyForValidContainer_ClearsPublicAccessOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            AddContainerAccessPolicy(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Container);

            client.SetContainerACL(containerName, ContainerAccessType.None, new List<BlobSignedIdentifier>() { });

            var actual = GetContainerPermissions(containerName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off, actual.PublicAccess);
        }

        [Test]
        [ExpectedException(typeof(ContainerNotFoundAzureException))]
        public void SetContainerACL_InvalidContainer_ThrowsContainerNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();

            client.SetContainerACL(containerName, ContainerAccessType.None, new List<BlobSignedIdentifier>() { });

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithContainerOperationAzureException))]
        public void SetContainerACL_LeaseForNonLeasedContainer_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            client.SetContainerACL(containerName, ContainerAccessType.None, new List<BlobSignedIdentifier>() { }, FakeLeaseId);

            var actual = GetContainerPermissions(containerName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off, actual.PublicAccess);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithContainerOperationAzureException))]
        public void SetContainerACL_WrongLeaseForLeasedContainer_ThrowsLeaseMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            var leaseId = LeaseContainer(containerName, null, null);

            client.SetContainerACL(containerName, ContainerAccessType.None, new List<BlobSignedIdentifier>() { }, FakeLeaseId);

            var actual = GetContainerPermissions(containerName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off, actual.PublicAccess);
        }

        [Test]
        public void SetContainerACL_LeaseForLeasedContainer_SetsPolicySuccesfully()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            string leaseId = LeaseContainer(containerName, null, null);
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

            var actual = GetContainerPermissions(containerName);
            Assert.AreEqual(1, actual.SharedAccessPolicies.Count);
        }

        [Test]
        public void SetContainerACL_NoLeaseForLeasedContainer_SetsPolicySuccesfully()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            string leaseId = LeaseContainer(containerName, null, null);
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

            var actual = GetContainerPermissions(containerName);
            Assert.AreEqual(1, actual.SharedAccessPolicies.Count);
        }

        [Test]
        public async Task SetContainerACLAsync_ReadPolicyForValidContainer_SetsPolicyAndPublicAccessOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
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

            var actual = GetContainerPermissions(containerName);
            Assert.AreEqual(1, actual.SharedAccessPolicies.Count);
            AssertIdentifierInSharedAccessPolicies(actual.SharedAccessPolicies, expectedIdentifier, Microsoft.WindowsAzure.Storage.Blob.SharedAccessBlobPermissions.Read);
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithContainerOperationAzureException))]
        public async Task SetContainerACLAsync_LeaseForNonLeasedContainer_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            await client.SetContainerACLAsync(containerName, ContainerAccessType.None, new List<BlobSignedIdentifier>() { }, FakeLeaseId);

            var actual = GetContainerPermissions(containerName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off, actual.PublicAccess);
        }

        [Test]
        public void DeleteContainer_ValidContainer_DeletesTheContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            client.DeleteContainer(containerName);

            AssertContainerDoesNotExist(containerName);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMissingAzureException))]
        public void DeleteContainer_NoLeaseForLeasedContainer_ThrowsLeaseIdMissingException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            var leaseId = LeaseContainer(containerName, null, null);

            client.DeleteContainer(containerName);

            AssertContainerDoesNotExist(containerName);
        }

        [Test]
        public void DeleteContainer_LeaseForLeasedContainer_DeletesContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            var leaseId = LeaseContainer(containerName, null, null);

            client.DeleteContainer(containerName, leaseId);

            AssertContainerDoesNotExist(containerName);
        }

        [Test]
        [ExpectedException(typeof(ContainerNotFoundAzureException))]
        public void DeleteContainer_NonExistentContainer_ThrowsContainerNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();

            client.DeleteContainer(containerName);

            AssertContainerDoesNotExist(containerName);
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithContainerOperationAzureException))]
        public void DeleteContainer_LeaseForNonLeasedContainer_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            client.DeleteContainer(containerName, FakeLeaseId);

            AssertContainerDoesNotExist(containerName);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithContainerOperationAzureException))]
        public void DeleteContainer_WrongLeaseForLeasedContainer_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            var leaseId = LeaseContainer(containerName, null, null);

            client.DeleteContainer(containerName, FakeLeaseId);

            AssertContainerDoesNotExist(containerName);
        }

        [Test]
        public async Task DeleteContainerAsync_ValidContainer_DeletesTheContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            await client.DeleteContainerAsync(containerName);

            AssertContainerDoesNotExist(containerName);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithContainerOperationAzureException))]
        public async Task DeleteContainerAsync_WrongLeaseForLeasedContainer_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            LeaseContainer(containerName, null, null);

            await client.DeleteContainerAsync(containerName, FakeLeaseId);

            AssertContainerDoesNotExist(containerName);
        }

        [Test]
        public void LeaseContainerAcquire_AcquireLeaseForValidContainer_AcquiresLease()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            var response = client.LeaseContainerAcquire(containerName, 30);

            AssertContainerIsLeased(containerName, response.LeaseId);
            RegisterContainerForCleanup(containerName, response.LeaseId);
        }

        [Test]
        public void LeaseContainerAcquire_AcquireInfiniteLeaseForValidContainer_AcquiresLease()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            var response = client.LeaseContainerAcquire(containerName, -1);

            AssertContainerIsLeased(containerName, response.LeaseId);
            RegisterContainerForCleanup(containerName, response.LeaseId);
        }

        [Test]
        public void LeaseContainerAcquire_AcquireSpecificLeaseIdForValidContainer_AcquiresLease()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            string expectedId = Guid.NewGuid().ToString();

            var response = client.LeaseContainerAcquire(containerName, 30, expectedId);

            Assert.AreEqual(expectedId, response.LeaseId);
            AssertContainerIsLeased(containerName, response.LeaseId);
            RegisterContainerForCleanup(containerName, response.LeaseId);
        }

        [Test]
        [ExpectedException(typeof(ContainerNotFoundAzureException))]
        public void LeaseContainerAcquire_InvalidContainer_ThrowsContainerNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();

            client.LeaseContainerAcquire(containerName, 30);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(LeaseAlreadyPresentAzureException))]
        public void LeaseContainerAcquire_AlreadyLeasedContainer_ThrowsAlreadyPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            LeaseContainer(containerName, null, null);

            client.LeaseContainerAcquire(containerName, 30);

            // expects exception
        }

        [Test]
        public async Task LeaseContainerAcquireAsync_AcquireLeaseForValidContainer_AcquiresLease()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            var response = await client.LeaseContainerAcquireAsync(containerName, 30);

            AssertContainerIsLeased(containerName, response.LeaseId);
            RegisterContainerForCleanup(containerName, response.LeaseId);
        }

        [Test]
        [ExpectedException(typeof(LeaseAlreadyPresentAzureException))]
        public async Task LeaseContainerAcquireAsync_AlreadyLeasedContainer_ThrowsAlreadyPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            LeaseContainer(containerName, null, null);

            await client.LeaseContainerAcquireAsync(containerName, 30);

            // expects exception
        }

        [Test]
        public void LeaseContainerRenew_LeasedContainer_RenewsActiveLease()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            var leaseId = LeaseContainer(containerName, TimeSpan.FromSeconds(30), null);

            client.LeaseContainerRenew(containerName, leaseId);

            // how do I test this?
            // it didn't blow up and it's still leased?
            AssertContainerIsLeased(containerName, leaseId);
        }

        [Test]
        [Ignore("This test is long by design because we have to wait for the lease to release before we attempt to renew")]
        public void LeaseContainerRenew_RecentlyLeasedContainer_RenewsLease()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            var leaseId = LeaseContainer(containerName, TimeSpan.FromSeconds(15), null);
            Thread.Sleep(16);

            client.LeaseContainerRenew(containerName, leaseId);

            // how do I test this?
            // it didn't blow up and it's still leased?
            AssertContainerIsLeased(containerName, leaseId);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithLeaseOperationAzureException))]
        public void LeaseContainerRenew_NonLeasedContainer_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            client.LeaseContainerRenew(containerName, FakeLeaseId);

            // expects exception
        }

        [Test]
        public async Task LeaseContainerRenewAsync_LeasedContainer_RenewsActiveLease()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            var leaseId = LeaseContainer(containerName, TimeSpan.FromSeconds(30), null);

            await client.LeaseContainerRenewAsync(containerName, leaseId);

            // how do I test this?
            // it didn't blow up and it's still leased?
            AssertContainerIsLeased(containerName, leaseId);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithLeaseOperationAzureException))]
        public async Task LeaseContainerRenewAsync_NonLeasedContainer_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            await client.LeaseContainerRenewAsync(containerName, FakeLeaseId);

            // expects exception
        }

        [Test]
        public void LeaseContainerChange_LeasedContainerToNewLeaseId_ChangesToMatchNewLeaseId()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            string leaseId = LeaseContainer(containerName, null, null);
            string expectedLeaseId = Guid.NewGuid().ToString();

            var response = client.LeaseContainerChange(containerName, leaseId, expectedLeaseId);

            Assert.AreEqual(expectedLeaseId, response.LeaseId);
            AssertContainerIsLeased(containerName, expectedLeaseId);
            RegisterContainerForCleanup(containerName, expectedLeaseId);
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithLeaseOperationAzureException))]
        public void LeaseContainerChange_NonLeasedContainer_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            string expectedLeaseId = Guid.NewGuid().ToString();

            var response = client.LeaseContainerChange(containerName, FakeLeaseId, expectedLeaseId);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(ContainerNotFoundAzureException))]
        public void LeaseContainerChange_NonexistentContainer_ThrowsContainerNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            string expectedLeaseId = Guid.NewGuid().ToString();

            var response = client.LeaseContainerChange(containerName, FakeLeaseId, expectedLeaseId);

            // expects exception
        }

        [Test]
        public async Task LeaseContainerChangeAsync_LeasedContainerToNewLeaseId_ChangesToMatchNewLeaseId()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            string leaseId = LeaseContainer(containerName, null, null);
            string expectedLeaseId = Guid.NewGuid().ToString();

            var response = await client.LeaseContainerChangeAsync(containerName, leaseId, expectedLeaseId);

            Assert.AreEqual(expectedLeaseId, response.LeaseId);
            AssertContainerIsLeased(containerName, expectedLeaseId);
            RegisterContainerForCleanup(containerName, expectedLeaseId);
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithLeaseOperationAzureException))]
        public async Task LeaseContainerChangeAsync_NonLeasedContainer_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            string expectedLeaseId = Guid.NewGuid().ToString();

            var response = await client.LeaseContainerChangeAsync(containerName, FakeLeaseId, expectedLeaseId);

            // expects exception
        }

        [Test]
        public void LeaseContainerRelease_LeasedContainer_ReleasesLease()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            var leaseId = LeaseContainer(containerName, null, null);

            client.LeaseContainerRelease(containerName, leaseId);

            AssertContainerIsNotLeased(containerName);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithLeaseOperationAzureException))]
        public void LeaseContainerRelease_NonLeasedContainer_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            client.LeaseContainerRelease(containerName, FakeLeaseId);

            // expects exception
        }


        [Test]
        public async Task LeaseContainerReleaseAsync_LeasedContainer_ReleasesLease()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            var leaseId = LeaseContainer(containerName, null, null);

            await client.LeaseContainerReleaseAsync(containerName, leaseId);

            AssertContainerIsNotLeased(containerName);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithLeaseOperationAzureException))]
        public async Task LeaseContainerReleaseAsync_NonLeasedContainer_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            await client.LeaseContainerReleaseAsync(containerName, FakeLeaseId);

            // expects exception
        }

        [Test]
        public void LeaseContainerBreak_LeasedContainer_BreaksLease()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            var leaseId = LeaseContainer(containerName, null, null);

            client.LeaseContainerBreak(containerName, leaseId, 0);

            var leaseState = GetContainerLeaseState(containerName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.LeaseState.Broken, leaseState);
        }

        [Test]
        public void LeaseContainerBreak_LeasedContainerWithLongBreakPeriod_SetLeaseToBreakinge()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            var leaseId = LeaseContainer(containerName, null, null);

            client.LeaseContainerBreak(containerName, leaseId, 60);

            var leaseState = GetContainerLeaseState(containerName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.LeaseState.Breaking, leaseState);
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithLeaseOperationAzureException))]
        public void LeaseContainerBreak_NonLeasedContainer_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            client.LeaseContainerBreak(containerName, FakeLeaseId, 0);

            // expects exception
        }

        [Test]
        public async Task LeaseContainerBreakAsync_LeasedContainerWithLongBreakPeriod_SetLeaseToBreakinge()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            var leaseId = LeaseContainer(containerName, null, null);

            await client.LeaseContainerBreakAsync(containerName, leaseId, 60);

            var leaseState = GetContainerLeaseState(containerName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.LeaseState.Breaking, leaseState);
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithLeaseOperationAzureException))]
        public async Task LeaseContainerBreakAsync_NonLeasedContainer_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            await client.LeaseContainerBreakAsync(containerName, FakeLeaseId, 0);

            // expects exception
        }

        [Test]
        public void ListBlobs_EmptyContainer_ReturnsEmptyList()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            var result = client.ListBlobs(containerName);

            Assert.IsEmpty(result.BlobList);
        }

        [Test]
        public void ListBlobs_PopulatedContainer_ReturnsExpectedBlobsInList()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, "blob/UnitTest/SampleA");
            CreateBlockBlob(containerName, "blob/UnitTest/SampleB");

            var result = client.ListBlobs(containerName);

            Assert.AreEqual(2, result.BlobList.Count);
            Assert.IsTrue(result.BlobList.Any(b => b.Name == "blob/UnitTest/SampleA"));
            Assert.IsTrue(result.BlobList.Any(b => b.Name == "blob/UnitTest/SampleB"));
        }

        [Test]
        public void ListBlobs_PrefixSupplied_ReturnsOnlyBlobsMatchingThatPrefix()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, "blob/UnitTest/SampleA");
            CreateBlockBlob(containerName, "blob/UnitTest/SampleB");
            CreateBlockBlob(containerName, "SomethingElse.txt");

            var result = client.ListBlobs(containerName, prefix: "blob");

            Assert.AreEqual(2, result.BlobList.Count);
            Assert.IsTrue(result.BlobList.Any(b => b.Name == "blob/UnitTest/SampleA"));
            Assert.IsTrue(result.BlobList.Any(b => b.Name == "blob/UnitTest/SampleB"));
        }

        [Test]
        public void ListBlobs_PrefixAndDelimiterSupplied_ReturnsOnlyBlobsMatchingThatPrefixWithNamesTruncatedAtDelimiter()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, "blob/UnitTest/SampleA");
            CreateBlockBlob(containerName, "blob/UnitTest/SampleB");
            CreateBlockBlob(containerName, "SomethingElse.txt");
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
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, "blob1");
            CreateBlockBlob(containerName, "blob2");
            CreateBlockBlob(containerName, "blob3");

            var result = client.ListBlobs(containerName, maxResults: 2);

            Assert.AreEqual(2, result.BlobList.Count);
            Assert.AreEqual(2, result.MaxResults);
            Assert.IsNotNullOrEmpty(result.NextMarker);
        }

        [Test]
        public void ListBlobs_MarkerSuppliedForLongerList_ReturnsNextSetofResults()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, "blob1");
            CreateBlockBlob(containerName, "blob2");
            CreateBlockBlob(containerName, "blob3");

            var result = client.ListBlobs(containerName, maxResults: 2);
            var nextMarker = result.NextMarker;
            var remainingResult = client.ListBlobs(containerName, marker: nextMarker);

            Assert.AreEqual(1, remainingResult.BlobList.Count);
            Assert.IsNotNullOrEmpty(remainingResult.Marker);
        }

        [Test]
        public void ListBlobs_IncludeMetadata_ReturnsMetadataInResults()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, "blob1", new Dictionary<string, string>() { 
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
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, "blob1");
            CopyBlob(containerName, "blob1", "blob1copy");

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
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, "blob1");
            SnapshotBlob(containerName, "blob1");

            var result = client.ListBlobs(containerName, include: ListBlobsInclude.Snapshots);

            Assert.AreEqual(2, result.BlobList.Count);
            Assert.IsTrue(result.BlobList.Any(b => b.Snapshot.HasValue));
            Assert.IsTrue(result.BlobList.Any(b => !b.Snapshot.HasValue));
        }

        [Test]
        public void ListBlobs_IncludeUncommittedBlobs_ReturnsUncommittedBlobs()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            CreateBlobUncommitted(containerName, "blob1");

            var result = client.ListBlobs(containerName, include: ListBlobsInclude.UncomittedBlobs);

            Assert.AreEqual(1, result.BlobList.Count);
        }

        [Test]
        public void ListBlobs_BlockAndPageBlobs_ReturnsBothTypes()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, "blob1");
            CreatePageBlob(containerName, "blob2");

            var result = client.ListBlobs(containerName);

            Assert.AreEqual(2, result.BlobList.Count);
            Assert.IsTrue(result.BlobList.Any(b => b.Properties.BlobType == Communications.Common.BlobType.Block));
            Assert.IsTrue(result.BlobList.Any(b => b.Properties.BlobType == Communications.Common.BlobType.Page));
        }

        [Test]
        public async Task ListBlobsAsync_PopulatedContainer_ReturnsExpectedBlobsInList()
        {
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, "blob/UnitTest/SampleA");
            CreateBlockBlob(containerName, "blob/UnitTest/SampleB");

            var result = await client.ListBlobsAsync(containerName);

            Assert.AreEqual(2, result.BlobList.Count);
            Assert.IsTrue(result.BlobList.Any(b => b.Name == "blob/UnitTest/SampleA"));
            Assert.IsTrue(result.BlobList.Any(b => b.Name == "blob/UnitTest/SampleB"));
        }

        #endregion

        #region Blob Operation Tests

        [Test]
        public void PutBlockBlob_RequiredArgsOnly_UploadsBlobSuccessfully()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");

            client.PutBlockBlob(containerName, blobName, data);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        public void PutBlockBlob_RequiredArgsOnlyAndBlobAlreadyExists_UploadsBlobSuccessfully()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");

            client.PutBlockBlob(containerName, blobName, data);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        public void PutBlockBlob_WithContentType_UploadsWithSpecifiedContentType()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            string expectedContentType = "text/plain";

            client.PutBlockBlob(containerName, blobName, data, contentType: expectedContentType);

            var blob = AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentType, blob.Properties.ContentType);
        }

        [Test]
        public void PutBlockBlob_WithContentEncoding_UploadsWithSpecifiedContentEncoding()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            string expectedContentEncoding = "UTF8";

            client.PutBlockBlob(containerName, blobName, data, contentEncoding: expectedContentEncoding);

            var blob = AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentEncoding, blob.Properties.ContentEncoding);
        }

        [Test]
        public void PutBlockBlob_WithContentLanguage_UploadsWithSpecifiedContentLanguage()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            string expectedContentLanguage = "gibberish";

            client.PutBlockBlob(containerName, blobName, data, contentLanguage: expectedContentLanguage);

            var blob = AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentLanguage, blob.Properties.ContentLanguage);
        }

        [Test]
        public void PutBlockBlob_WithContentMD5_UploadsWithSpecifiedContentMD5()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            var expectedContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(data));

            client.PutBlockBlob(containerName, blobName, data, contentMD5: expectedContentMD5);

            var blob = AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            //this next test is not a real one, just for roundtrip verification
            Assert.AreEqual(expectedContentMD5, blob.Properties.ContentMD5);
        }

        [Test]
        [ExpectedException(typeof(Md5MismatchAzureException))]
        public void PutBlockBlob_WithIncorrectContentMD5_ThrowsMD5MismatchAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            var someOtherData = UTF8Encoding.UTF8.GetBytes("different content");
            var incorrectContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(someOtherData));

            client.PutBlockBlob(containerName, blobName, data, contentMD5: incorrectContentMD5);

            // expects exception
        }

        [Test]
        public void PutBlockBlob_WithCacheControl_UploadsWithCacheControl()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            string expectedCacheControl = "123-ABC";

            client.PutBlockBlob(containerName, blobName, data, cacheControl: expectedCacheControl);

            var blob = AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedCacheControl, blob.Properties.CacheControl);
        }

        [Test]
        public void PutBlockBlob_WithMetadata_UploadsMetadata()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            var expectedMetadata = new Dictionary<string, string>(){
                { "firstValue", "1" },
                { "secondValue", "2"}
            };

            client.PutBlockBlob(containerName, blobName, data, metadata: expectedMetadata);

            var blob = AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.IsTrue(blob.Metadata.Any(m => m.Key == "firstValue" && m.Value == "1"), "First value is missing or incorrect");
            Assert.IsTrue(blob.Metadata.Any(m => m.Key == "secondValue" && m.Value == "2"), "Second value is missing or incorrect");
        }


        [Test]
        public async Task PutBlockBlobAsync_RequiredArgsOnly_UploadsBlobSuccessfully()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");

            await client.PutBlockBlobAsync(containerName, blobName, data);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        [ExpectedException(typeof(Md5MismatchAzureException))]
        public async Task PutBlockBlobAsync_WithIncorrectContentMD5_ThrowsMD5MismatchAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            var someOtherData = UTF8Encoding.UTF8.GetBytes("different content");
            var incorrectContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(someOtherData));

            await client.PutBlockBlobAsync(containerName, blobName, data, contentMD5: incorrectContentMD5);

            // expects exception
        }

        [Test]
        public void PutBlock_RequiredArgsOnly_UploadsBlockSuccessfully()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            client.PutBlock(containerName, blobName, blockId, data);

            AssertBlockExists(containerName, blobName, blockId);
        }

        [Test]
        public void PutBlock_WithContentMD5_UploadsWithSpecifiedContentMD5()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var expectedContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(data));
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            var block = client.PutBlock(containerName, blobName, blockId, data, contentMD5: expectedContentMD5);

            AssertBlockExists(containerName, blobName, blockId);
            Assert.AreEqual(expectedContentMD5, block.ContentMD5);
        }

        [Test]
        [ExpectedException(typeof(Md5MismatchAzureException))]
        public void PutBlock_WithIncorrectContentMD5_ThrowsMD5MismatchAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var someOtherData = UTF8Encoding.UTF8.GetBytes("different content");
            var incorrectContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(someOtherData));
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            client.PutBlock(containerName, blobName, blockId, data, contentMD5: incorrectContentMD5);

            // expects exception
        }

        [Test]
        public void PutBlock_RequiredArgsOnly_ReturnsCorrectMD5Hash()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            var expectedContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(data));

            var response = client.PutBlock(containerName, blobName, blockId, data);

            Assert.AreEqual(expectedContentMD5, response.ContentMD5);
        }

        [Test]
        [ExpectedException(typeof(RequestBodyTooLargeAzureException))]
        public void PutBlock_TooLargePayload_ThrowsRequestBodyTooLargeAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var fiveMegabytes = new byte[5242880];
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            client.PutBlock(containerName, blobName, blockId, fiveMegabytes);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void PutBlock_DifferentLengthBlockIds_ThrowsArgumentException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var differentLengthBlockId = "test-block-id-wrong-length";
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");

            client.PutBlock(containerName, blobName, blockId, data);
            client.PutBlock(containerName, blobName, differentLengthBlockId, data);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void PutBlock_BlockIdTooLarge_ThrowsArgumentException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockId = Base64Converter.ConvertToBase64("test-block-id-very-long-too-long-horribly-wrong-does-not-compute-danger-will-robinson");
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");

            client.PutBlock(containerName, blobName, blockId, data);

            // throws exception
        }

        [Test]
        public void PutPageBlob_WithRequiredArgs_CreatesNewPageBlob()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            int expectedSize = 512;

            client.PutPageBlob(containerName, blobName, expectedSize);

            var blob = AssertBlobExists(containerName, blobName, BlobType.PageBlob);
            Assert.AreEqual(expectedSize, blob.Properties.Length);
        }

        [Test]
        public void PutPageBlob_WithContentType_UploadsWithSpecifiedContentType()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            string expectedContentType = "text/plain";
            int expectedSize = 512;

            client.PutPageBlob(containerName, blobName, expectedSize, contentType: expectedContentType);

            var blob = AssertBlobExists(containerName, blobName, BlobType.PageBlob);
            Assert.AreEqual(expectedContentType, blob.Properties.ContentType);
        }

        [Test]
        public void PutPageBlob_WithContentEncoding_UploadsWithSpecifiedContentEncoding()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            string expectedContentEncoding = "UTF8";
            int expectedSize = 512;

            client.PutPageBlob(containerName, blobName, expectedSize, contentEncoding: expectedContentEncoding);

            var blob = AssertBlobExists(containerName, blobName, BlobType.PageBlob);
            Assert.AreEqual(expectedContentEncoding, blob.Properties.ContentEncoding);
        }

        [Test]
        public void PutPageBlob_WithContentLanguage_UploadsWithSpecifiedContentLanguage()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            string expectedContentLanguage = "gibberish";
            int expectedSize = 512;

            client.PutPageBlob(containerName, blobName, expectedSize, contentLanguage: expectedContentLanguage);

            var blob = AssertBlobExists(containerName, blobName, BlobType.PageBlob);
            Assert.AreEqual(expectedContentLanguage, blob.Properties.ContentLanguage);
        }

        [Test]
        public void PutPageBlob_WithCacheControl_UploadsWithCacheControl()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            string expectedCacheControl = "123-ABC";
            int expectedSize = 512;

            client.PutPageBlob(containerName, blobName, expectedSize, cacheControl: expectedCacheControl);

            var blob = AssertBlobExists(containerName, blobName, BlobType.PageBlob);
            Assert.AreEqual(expectedCacheControl, blob.Properties.CacheControl);
        }

        [Test]
        public void PutPageBlob_WithMetadata_UploadsMetadata()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            var expectedMetadata = new Dictionary<string, string>(){
                { "firstValue", "1" },
                { "secondValue", "2"}
            };
            int expectedSize = 512;

            client.PutPageBlob(containerName, blobName, expectedSize, metadata: expectedMetadata);

            var blob = AssertBlobExists(containerName, blobName, BlobType.PageBlob);
            Assert.IsTrue(blob.Metadata.Any(m => m.Key == "firstValue" && m.Value == "1"), "First value is missing or incorrect");
            Assert.IsTrue(blob.Metadata.Any(m => m.Key == "secondValue" && m.Value == "2"), "Second value is missing or incorrect");
        }

        [Test]
        public void PutPageBlob_WithSequenceNumber_AssignsSpecifiedSequenceNumberToBlob()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            int expectedSize = 512;
            long expectedSequenceNumber = 123;

            client.PutPageBlob(containerName, blobName, expectedSize, sequenceNumber: expectedSequenceNumber);

            var blob = AssertBlobExists(containerName, blobName, BlobType.PageBlob);
            Assert.AreEqual(expectedSequenceNumber, blob.Properties.PageBlobSequenceNumber);
        }

        [Test]
        public async Task PutPageBlobAsync_WithRequiredArgs_CreatesNewPageBlob()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            int expectedSize = 512;

            await client.PutPageBlobAsync(containerName, blobName, expectedSize);

            var blob = AssertBlobExists(containerName, blobName, BlobType.PageBlob);
            Assert.AreEqual(expectedSize, blob.Properties.Length);
        }

        [Test]
        public void DeleteBlob_ExistingBlob_DeletesBlob()
        {
            var expectedContent = "Expected blob content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName, content: expectedContent);
            var client = new BlobServiceClient(_accountSettings);

            client.DeleteBlob(containerName, blobName);

            AssertBlobDoesNotExist(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        public async Task DeleteBlobAsync_ExistingBlob_DeletesBlobAsynchronously()
        {
            var expectedContent = "Expected blob content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName, content: expectedContent);
            var client = new BlobServiceClient(_accountSettings);

            await client.DeleteBlobAsync(containerName, blobName);

            AssertBlobDoesNotExist(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        [ExpectedException(typeof(BlobNotFoundAzureException))]
        public void DeleteBlob_NonExistingBlob_ThrowsBlobDoesNotExistException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);

            // delete blob that doesn't exist => should throw an exception
            client.DeleteBlob(containerName, blobName);
        }

        [Test]
        [ExpectedException(typeof(BlobNotFoundAzureException))]
        public async void DeleteBlobAsync_NonExistingBlob_ThrowsBlobDoesNotExistException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);

            // delete blog that doesn't exist => should throw an exception
            await client.DeleteBlobAsync(containerName, blobName);
        }

        [Test]
        public void GetBlob_ExistingBlob_DownloadsBlobBytes()
        {
            var expectedContent = "Expected blob content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            var response = client.GetBlob(containerName, blobName);
            var data = response.GetDataBytes();

            Assert.AreEqual(expectedContent, UTF8Encoding.UTF8.GetString(data));
        }

        [Test]
        public async Task GetBlobAsync_ExistingBlob_DownloadsBlobBytes()
        {
            var expectedContent = "Expected blob content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            var response = await client.GetBlobAsync(containerName, blobName);
            var data = response.GetDataBytes();

            Assert.AreEqual(expectedContent, UTF8Encoding.UTF8.GetString(data));
        }

        [Test]
        public void GetBlob_ExistingBlob_DownloadsBlobStream()
        {
            var expectedContent = "Expected blob content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            var response = client.GetBlob(containerName, blobName);
            byte[] data;
            using (var stream = response.GetDataStream())
            {
                var ms = new MemoryStream();
                stream.CopyTo(ms);
                data = ms.ToArray();
            }

            Assert.AreEqual(expectedContent, UTF8Encoding.UTF8.GetString(data));
        }

        [Test]
        public async Task GetBlobAsync_ExistingBlob_DownloadsBlobStream()
        {
            var expectedContent = "Expected blob content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            var response = await client.GetBlobAsync(containerName, blobName);
            byte[] data;
            using (var stream = response.GetDataStream())
            {
                var ms = new MemoryStream();
                stream.CopyTo(ms);
                data = ms.ToArray();
            }

            Assert.AreEqual(expectedContent, UTF8Encoding.UTF8.GetString(data));
        }

        [Test]
        [ExpectedException(typeof(BlobNotFoundAzureException))]
        public void GetBlob_NonExistentBlob_ThrowsBlobDoesNotExistException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            var response = client.GetBlob(containerName, blobName);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(BlobNotFoundAzureException))]
        public async Task GetBlobAsync_NonExistentBlob_ThrowsBlobDoesNotExistException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            var response = await client.GetBlobAsync(containerName, blobName);

            // expects exception
        }

        [Test]
        public void GetBlob_ExistingBlobByValidStartRange_DownloadBlobRangeOnly()
        {
            var expectedContent = "Expected blob content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            var response = client.GetBlob(containerName, blobName, range: new BlobRange(2));
            var data = response.GetDataBytes();

            Assert.AreEqual(expectedContent.Substring(2), UTF8Encoding.UTF8.GetString(data));
        }

        [Test]
        public async void GetBlobAsync_ExistingBlobByValidStartRange_DownloadBlobRangeOnly()
        {
            var expectedContent = "Expected blob content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            var response = await client.GetBlobAsync(containerName, blobName, range: new BlobRange(2));
            var data = response.GetDataBytes();

            Assert.AreEqual(expectedContent.Substring(2), UTF8Encoding.UTF8.GetString(data));
        }

        [Test]
        public void GetBlob_ExistingBlobByValidStartAndEndRange_DownloadBlobRangeOnly()
        {
            var expectedContent = "Expected blob content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            var response = client.GetBlob(containerName, blobName, range: new BlobRange(2, 6));
            var data = response.GetDataBytes();

            Assert.AreEqual(expectedContent.Substring(2, 5), UTF8Encoding.UTF8.GetString(data));
        }

        [Test]
        public async void GetBlobAsync_ExistingBlobByValidStartAndEndRange_DownloadBlobRangeOnly()
        {
            var expectedContent = "Expected blob content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            var response = await client.GetBlobAsync(containerName, blobName, range: new BlobRange(2, 6));
            var data = response.GetDataBytes();

            Assert.AreEqual(expectedContent.Substring(2, 5), UTF8Encoding.UTF8.GetString(data));
        }

        //[Test]
        //public void GetBlobProperties_ValidBlob_ReturnsProperties()
        //{
        //    IBlobServiceClient client = new BlobServiceClient(_accountSettings);
        //    var containerName = GenerateSampleContainerName();
        //    CreateContainer(containerName);
        //    CreateBlockBlob(containerName, "blob1");

        //    var response = client.GetProperties(containerName, "blob1");
        //}

        #endregion

        #region Assertions

        private void AssertContainerExists(string containerName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail(String.Format("AssertContainerExists: The container '{0}' does not exist", containerName));
        }

        private void AssertContainerDoesNotExist(string containerName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (container.Exists())
                Assert.Fail(String.Format("AssertContainerDoesNotExist: The container '{0}' exists", containerName));
        }

        private void AssertContainerIsLeased(string containerName, string leaseId)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail(String.Format("AssertContainerIsLeased: The container '{0}' does not exist", containerName));

            try
            {
                container.RenewLease(new AccessCondition() { LeaseId = leaseId });
            }
            catch (Exception exc)
            {
                Assert.Fail(String.Format("AssertContainerIsLeased: The container '{0}' gave an {1} exception when renewing with the specified lease id: {2}", containerName, exc.GetType().Name, exc.Message));
            }
        }

        private void AssertContainerIsNotLeased(string containerName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail(String.Format("AssertContainerIsNotLeased: The container '{0}' does not exist", containerName));

            try
            {
                var leaseId = container.AcquireLease(null, null);
                RegisterContainerForCleanup(containerName, leaseId);
            }
            catch (Exception exc)
            {
                Assert.Fail(String.Format("AssertContainerIsNotLeased: The container '{0}' gave an {1} exception when attempting to acquire a new lease: {2}", containerName, exc.GetType().Name, exc.Message));
            }
        }

        private void AssertContainerAccess(string containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType containerAccessType)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail(String.Format("AssertContainerAccess: The container '{0}' does not exist", containerName));

            var permissions = container.GetPermissions();
            Assert.AreEqual(containerAccessType, permissions.PublicAccess, String.Format("AssertContainerAccess: Container access was expected to be {0}, but it is actually {1}", containerAccessType, permissions.PublicAccess));
        }

        private Microsoft.WindowsAzure.Storage.Blob.ICloudBlob AssertBlobExists(string containerName, string blobName, BlobType blobType)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail("AssertBlobExists: The container '{0}' does not exist", containerName);

            var blob = (blobType == BlobType.BlockBlob
                ? (ICloudBlob)container.GetBlockBlobReference(blobName)
                : (ICloudBlob)container.GetPageBlobReference(blobName));

            if (!blob.Exists())
                Assert.Fail("AssertBlobExists: The blob '{0}' does not exist", blobName);

            return blob;
        }

        private Microsoft.WindowsAzure.Storage.Blob.ListBlockItem AssertBlockExists(string containerName, string blobName, string blockId, BlockListingFilter blockType = BlockListingFilter.All)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail("AssertBlockExists: The container '{0}' does not exist", containerName);

            var blob = container.GetBlockBlobReference(blobName);
            var blockList = blob.DownloadBlockList(blockType);
            var block = blockList.FirstOrDefault(item => item.Name == blockId);

            if (block == null)
                Assert.Fail("AssertBlockExists: The block of id '{0}' does not exist", blockId);

            return block;
        }

        private Microsoft.WindowsAzure.Storage.Blob.ICloudBlob AssertBlobDoesNotExist(string containerName, string blobName, BlobType blobType)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail("AssertBlobDoesNotExist: The container '{0}' does not exist", containerName);

            var blob = (blobType == BlobType.BlockBlob
                ? (ICloudBlob)container.GetBlockBlobReference(blobName)
                : (ICloudBlob)container.GetPageBlobReference(blobName));

            if (blob.Exists())
                Assert.Fail("AssertBlobDoesNotExist: The blob '{0}' exists", blobName);

            return blob;

        }

        private IDictionary<string, string> GetContainerMetadata(string containerName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);

            container.FetchAttributes();
            return container.Metadata;
        }

        private void AssertIdentifierInSharedAccessPolicies(Microsoft.WindowsAzure.Storage.Blob.SharedAccessBlobPolicies sharedAccessPolicies, BlobSignedIdentifier expectedIdentifier, Microsoft.WindowsAzure.Storage.Blob.SharedAccessBlobPermissions permissions)
        {
            var policy = sharedAccessPolicies.Where(i => i.Key.Equals(expectedIdentifier.Id, StringComparison.InvariantCultureIgnoreCase)).FirstOrDefault();
            Assert.IsNotNull(policy);
            Assert.AreEqual(expectedIdentifier.AccessPolicy.StartTime, policy.Value.SharedAccessStartTime.Value.UtcDateTime);
            Assert.AreEqual(expectedIdentifier.AccessPolicy.Expiry, policy.Value.SharedAccessExpiryTime.Value.UtcDateTime);
            Assert.IsTrue(policy.Value.Permissions.HasFlag(permissions));
        }

        #endregion

        #region Setup Mechanics

        private void CreateContainer(string containerName, Dictionary<string, string> metadata = null)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);

            container.Create();

            if (metadata != null)
            {
                // why ???
                foreach (var key in metadata.Keys)
                {
                    container.Metadata.Add(key, metadata[key]);
                }
                container.SetMetadata();
            }

        }

        private string LeaseContainer(string containerName, TimeSpan? leaseTime, string leaseId)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var lease = container.AcquireLease(leaseTime, leaseId);
            RegisterContainerForCleanup(containerName, lease);
            return lease;
        }

        private void ReleaseContainerLease(string containerName, string lease)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            container.ReleaseLease(new AccessCondition() { LeaseId = lease });
        }

        private void BreakContainerLease(string containerName, string lease)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            container.BreakLease(TimeSpan.FromSeconds(1), new AccessCondition() { LeaseId = lease });
        }

        private void AddContainerAccessPolicy(string containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType publicAccess, string id = null, DateTime? startDate = null, DateTime? expiry = null)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var permissions = container.GetPermissions();
            permissions.PublicAccess = publicAccess;
            if (!string.IsNullOrEmpty(id) && startDate.HasValue && expiry.HasValue)
            {
                permissions.SharedAccessPolicies.Add(id, new Microsoft.WindowsAzure.Storage.Blob.SharedAccessBlobPolicy()
                {
                    Permissions = Microsoft.WindowsAzure.Storage.Blob.SharedAccessBlobPermissions.Read,
                    SharedAccessStartTime = startDate,
                    SharedAccessExpiryTime = expiry
                });
            }
            container.SetPermissions(permissions);
        }

        private Microsoft.WindowsAzure.Storage.Blob.BlobContainerPermissions GetContainerPermissions(string containerName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var permissions = container.GetPermissions();
            return permissions;
        }

        private Microsoft.WindowsAzure.Storage.Blob.LeaseState GetContainerLeaseState(string containerName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            container.FetchAttributes();
            return container.Properties.LeaseState;
        }

        private void CreateBlockBlob(string containerName, string blobName, Dictionary<string, string> metadata = null, string content = "Generic content")
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);

            byte[] data = UTF8Encoding.UTF8.GetBytes(content);
            blob.UploadFromByteArray(data, 0, data.Length);

            if (metadata != null)
            {
                foreach (var key in metadata.Keys)
                {
                    blob.Metadata.Add(key, metadata[key]);
                }
                blob.SetMetadata();
            }
        }

        private void CreatePageBlob(string containerName, string blobName, Dictionary<string, string> metadata = null)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetPageBlobReference(blobName);

            byte[] data = UTF8Encoding.UTF8.GetBytes("Generic content");
            var necessarySize = data.Length + (512 - (data.Length % 512));
            var pageData = new byte[necessarySize];
            data.CopyTo(pageData, 0);
            blob.UploadFromByteArray(pageData, 0, pageData.Length);

            if (metadata != null)
            {
                foreach (var key in metadata.Keys)
                {
                    blob.Metadata.Add(key, metadata[key]);
                }
                blob.SetMetadata();
            }
        }

        private void CreateBlobUncommitted(string containerName, string blobName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);

            byte[] data = UTF8Encoding.UTF8.GetBytes("Generic content");
            // non-Base64 values fail?
            var blockId = Convert.ToBase64String(Encoding.UTF8.GetBytes("A"));
            blob.PutBlock(blockId, new MemoryStream(data), null);
        }

        private void CopyBlob(string containerName, string sourceBlobName, string targetBlobName)
        {
            // could we have made this require more work?
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var sourceBlob = container.GetBlockBlobReference(sourceBlobName);
            var targetBlob = container.GetBlockBlobReference(targetBlobName);
            targetBlob.StartCopyFromBlob(sourceBlob);
        }

        private void SnapshotBlob(string containerName, string blobName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);
            blob.CreateSnapshot();
        }


        #endregion

        private DateTime GetTruncatedUtcNow()
        {
            return new DateTime(DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day, DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, DateTime.UtcNow.Second, DateTimeKind.Utc);
        }
    }
}
