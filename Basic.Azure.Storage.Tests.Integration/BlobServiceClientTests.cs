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
using BlobType = Microsoft.WindowsAzure.Storage.Blob.BlobType;
using LeaseDuration = Basic.Azure.Storage.Communications.Common.LeaseDuration;
using LeaseState = Basic.Azure.Storage.Communications.Common.LeaseState;
using LeaseStatus = Basic.Azure.Storage.Communications.Common.LeaseStatus;

namespace Basic.Azure.Storage.Tests.Integration
{
    [TestFixture]
    public class BlobServiceClientTests : BaseBlobServiceClientTestFixture
    {

        #region Account Operations

        #endregion

        #region Container Operations Tests

        [Test]
        public void CreateContainer_ValidArguments_CreatesContainerWithSpecificName()
        {
            var containerName = GenerateSampleContainerName();
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.CreateContainer(containerName, ContainerAccessType.None);

            AssertContainerExists(containerName);
        }

        [Test]
        public void CreateContainer_ValidArgumentsWithPublicContainerAccess_CreatesContainerWithContainerAccess()
        {
            var containerName = GenerateSampleContainerName();
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.CreateContainer(containerName, ContainerAccessType.PublicContainer);

            AssertContainerAccess(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Container);
        }

        [Test]
        public void CreateContainer_ValidArgumentsWithPublicBlobAccess_CreatesContainerWithBlobAccess()
        {
            var containerName = GenerateSampleContainerName();
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.CreateContainer(containerName, ContainerAccessType.PublicBlob);

            AssertContainerAccess(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Blob);
        }

        [Test]
        public void CreateContainer_ValidArgumentsWithNoPublicAccess_CreatesContainerWithNoPublicAccess()
        {
            var containerName = GenerateSampleContainerName();
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.CreateContainer(containerName, ContainerAccessType.None);

            AssertContainerAccess(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off);
        }

        [Test]
        [ExpectedException(typeof(ContainerAlreadyExistsAzureException))]
        public void CreateContainer_AlreadyExists_ThrowsContainerAlreadyExistsException()
        {
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.CreateContainer(containerName, ContainerAccessType.None);

            // expects exception
        }

        [Test]
        public void CreateContainer_ValidArguments_ReturnsContainerCreationResponse()
        {
            var containerName = GenerateSampleContainerName();
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.CreateContainer(containerName, ContainerAccessType.None);

            Assert.IsTrue(response.Date > DateTime.Now.AddSeconds(-5), String.Format("Response Date was set to {0}", response.Date));
            Assert.IsTrue(response.LastModified > DateTime.Now.AddSeconds(-5), String.Format("Response LastModified was set to {0}", response.LastModified));
            Assert.IsFalse(string.IsNullOrWhiteSpace(response.ETag), "Response ETag is not set");

        }

        [Test]
        public async Task CreateContainerAsync_ValidArguments_CreatesContainerWithSpecificName()
        {
            var containerName = GenerateSampleContainerName();
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.CreateContainerAsync(containerName, ContainerAccessType.None);

            AssertContainerExists(containerName);
        }

        [Test]
        [ExpectedException(typeof(ContainerAlreadyExistsAzureException))]
        public async Task CreateContainerAsync_AlreadyExists_ThrowsContainerAlreadyExistsException()
        {
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.CreateContainerAsync(containerName, ContainerAccessType.None);

            // expects exception
        }

        [Test]
        public void GetContainerProperties_ValidContainer_ReturnsProperties()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();

            client.GetContainerProperties(containerName);

            // expects exception
        }

        [Test]
        public void GetContainerProperties_ContainerWithMetadata_ReturnsMetadata()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            LeaseContainer(containerName, TimeSpan.FromSeconds(30), null);

            var response = client.GetContainerProperties(containerName);

            Assert.AreEqual(LeaseStatus.Locked, response.LeaseStatus);
            Assert.AreEqual(LeaseDuration.Fixed, response.LeaseDuration);
            Assert.AreEqual(LeaseState.Leased, response.LeaseState);
        }

        [Test]
        public void GetContainerProperties_InfiniteLeaseContainer_ReturnsLeaseDetails()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            const int leaseLength = 30;
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            string lease = LeaseContainer(containerName, TimeSpan.FromSeconds(leaseLength), null);
            BreakContainerLease(containerName, lease, leaseLength);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();

            await client.GetContainerPropertiesAsync(containerName);

            // expects exception
        }

        [Test]
        public void GetContainerMetadata_ValidContainer_ReturnsMetadata()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName, new Dictionary<string, string>());

            var response = client.GetContainerMetadata(containerName);

            Assert.IsNotNull(response.Metadata);
            Assert.AreEqual(0, response.Metadata.Count);
        }

        [Test]
        [ExpectedException(typeof(ContainerNotFoundAzureException))]
        public void GetContainerMetadata_NonexistentContainer_ThrowsContainerNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();

            client.GetContainerMetadata(containerName);

            //expects exception
        }

        [Test]
        public async Task GetContainerMetadataAsync_ValidContainer_ReturnsMetadata()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();

            await client.GetContainerMetadataAsync(containerName);

            //expects exception
        }

        [Test]
        public void SetContainerMetadata_ValidContainer_SetsMetadataOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            var result = client.GetContainerACL(containerName);

            Assert.IsEmpty(result.SignedIdentifiers);
        }

        [Test]
        public void GetContainerACL_HasAccessPolicies_ReturnsListConstainingThosePolicies()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            AddContainerAccessPolicy(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Container);

            var result = client.GetContainerACL(containerName);

            Assert.AreEqual(ContainerAccessType.PublicContainer, result.PublicAccess);
        }

        [Test]
        public void GetContainerACL_NoPublicAccess_ReturnsPublicAccessAsNone()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();

            client.GetContainerACL(containerName);

            // expects exception
        }

        [Test]
        public async Task GetContainerACLAsync_HasAccessPolicies_ReturnsListConstainingThosePolicies()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();

            await client.GetContainerACLAsync(containerName);

            // expects exception
        }

        [Test]
        public void SetContainerACL_ReadPolicyForValidContainer_SetsPolicyAndPublicAccessOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            client.SetContainerACL(containerName, ContainerAccessType.PublicBlob, new List<BlobSignedIdentifier>());

            var actual = GetContainerPermissions(containerName);
            Assert.AreEqual(0, actual.SharedAccessPolicies.Count);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Blob, actual.PublicAccess);
        }

        [Test]
        public void SetContainerACL_NoPublicAccessAndPolicyForValidContainer_ClearsPublicAccessOnContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            AddContainerAccessPolicy(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Container);

            client.SetContainerACL(containerName, ContainerAccessType.None, new List<BlobSignedIdentifier>());

            var actual = GetContainerPermissions(containerName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off, actual.PublicAccess);
        }

        [Test]
        [ExpectedException(typeof(ContainerNotFoundAzureException))]
        public void SetContainerACL_InvalidContainer_ThrowsContainerNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();

            client.SetContainerACL(containerName, ContainerAccessType.None, new List<BlobSignedIdentifier>());

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithContainerOperationAzureException))]
        public void SetContainerACL_LeaseForNonLeasedContainer_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            client.SetContainerACL(containerName, ContainerAccessType.None, new List<BlobSignedIdentifier>(), FakeLeaseId);

            var actual = GetContainerPermissions(containerName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off, actual.PublicAccess);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithContainerOperationAzureException))]
        public void SetContainerACL_WrongLeaseForLeasedContainer_ThrowsLeaseMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            LeaseContainer(containerName, null, null);

            client.SetContainerACL(containerName, ContainerAccessType.None, new List<BlobSignedIdentifier>(), FakeLeaseId);

            var actual = GetContainerPermissions(containerName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off, actual.PublicAccess);
        }

        [Test]
        public void SetContainerACL_LeaseForLeasedContainer_SetsPolicySuccesfully()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            LeaseContainer(containerName, null, null);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            await client.SetContainerACLAsync(containerName, ContainerAccessType.None, new List<BlobSignedIdentifier>(), FakeLeaseId);

            var actual = GetContainerPermissions(containerName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off, actual.PublicAccess);
        }

        [Test]
        public void DeleteContainer_ValidContainer_DeletesTheContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            client.DeleteContainer(containerName);

            AssertContainerDoesNotExist(containerName);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMissingAzureException))]
        public void DeleteContainer_NoLeaseForLeasedContainer_ThrowsLeaseIdMissingException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            LeaseContainer(containerName, null, null);

            client.DeleteContainer(containerName);

            AssertContainerDoesNotExist(containerName);
        }

        [Test]
        public void DeleteContainer_LeaseForLeasedContainer_DeletesContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();

            client.DeleteContainer(containerName);

            AssertContainerDoesNotExist(containerName);
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithContainerOperationAzureException))]
        public void DeleteContainer_LeaseForNonLeasedContainer_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            client.DeleteContainer(containerName, FakeLeaseId);

            AssertContainerDoesNotExist(containerName);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithContainerOperationAzureException))]
        public void DeleteContainer_WrongLeaseForLeasedContainer_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            LeaseContainer(containerName, null, null);

            client.DeleteContainer(containerName, FakeLeaseId);

            AssertContainerDoesNotExist(containerName);
        }

        [Test]
        public async Task DeleteContainerAsync_ValidContainer_DeletesTheContainer()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            await client.DeleteContainerAsync(containerName);

            AssertContainerDoesNotExist(containerName);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithContainerOperationAzureException))]
        public async Task DeleteContainerAsync_WrongLeaseForLeasedContainer_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            LeaseContainer(containerName, null, null);

            await client.DeleteContainerAsync(containerName, FakeLeaseId);

            AssertContainerDoesNotExist(containerName);
        }

        [Test]
        public void LeaseContainerAcquire_AcquireLeaseForValidContainer_AcquiresLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            var response = client.LeaseContainerAcquire(containerName, 30);

            AssertContainerIsLeased(containerName, response.LeaseId);
            RegisterContainerForCleanup(containerName, response.LeaseId);
        }

        [Test]
        public void LeaseContainerAcquire_AcquireInfiniteLeaseForValidContainer_AcquiresLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            var response = client.LeaseContainerAcquire(containerName, -1);

            AssertContainerIsLeased(containerName, response.LeaseId);
            RegisterContainerForCleanup(containerName, response.LeaseId);
        }

        [Test]
        public void LeaseContainerAcquire_AcquireSpecificLeaseIdForValidContainer_AcquiresLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();

            client.LeaseContainerAcquire(containerName, 30);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(LeaseAlreadyPresentAzureException))]
        public void LeaseContainerAcquire_AlreadyLeasedContainer_ThrowsAlreadyPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            LeaseContainer(containerName, null, null);

            client.LeaseContainerAcquire(containerName, 30);

            // expects exception
        }

        [Test]
        public async Task LeaseContainerAcquireAsync_AcquireLeaseForValidContainer_AcquiresLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            LeaseContainer(containerName, null, null);

            await client.LeaseContainerAcquireAsync(containerName, 30);

            // expects exception
        }

        [Test]
        public void LeaseContainerRenew_LeasedContainer_RenewsActiveLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            client.LeaseContainerRenew(containerName, FakeLeaseId);

            // expects exception
        }

        [Test]
        public async Task LeaseContainerRenewAsync_LeasedContainer_RenewsActiveLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            await client.LeaseContainerRenewAsync(containerName, FakeLeaseId);

            // expects exception
        }

        [Test]
        public void LeaseContainerChange_LeasedContainerToNewLeaseId_ChangesToMatchNewLeaseId()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            client.LeaseContainerChange(containerName, FakeLeaseId, expectedLeaseId);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(ContainerNotFoundAzureException))]
        public void LeaseContainerChange_NonexistentContainer_ThrowsContainerNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var expectedLeaseId = Guid.NewGuid().ToString();

            client.LeaseContainerChange(containerName, FakeLeaseId, expectedLeaseId);

            // expects exception
        }

        [Test]
        public async Task LeaseContainerChangeAsync_LeasedContainerToNewLeaseId_ChangesToMatchNewLeaseId()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            await client.LeaseContainerChangeAsync(containerName, FakeLeaseId, expectedLeaseId);

            // expects exception
        }

        [Test]
        public void LeaseContainerRelease_LeasedContainer_ReleasesLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            client.LeaseContainerRelease(containerName, FakeLeaseId);

            // expects exception
        }


        [Test]
        public async Task LeaseContainerReleaseAsync_LeasedContainer_ReleasesLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            await client.LeaseContainerReleaseAsync(containerName, FakeLeaseId);

            // expects exception
        }

        [Test]
        public void LeaseContainerBreak_LeasedContainer_BreaksLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            client.LeaseContainerBreak(containerName, FakeLeaseId, 0);

            // expects exception
        }

        [Test]
        public async Task LeaseContainerBreakAsync_LeasedContainerWithLongBreakPeriod_SetLeaseToBreakinge()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            await client.LeaseContainerBreakAsync(containerName, FakeLeaseId, 0);

            // expects exception
        }

        [Test]
        public void ListBlobs_EmptyContainer_ReturnsEmptyList()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            var result = client.ListBlobs(containerName);

            Assert.IsEmpty(result.BlobList);
        }

        [Test]
        public void ListBlobs_PopulatedContainer_ReturnsExpectedBlobsInList()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            CreateBlobUncommitted(containerName, "blob1");

            var result = client.ListBlobs(containerName, include: ListBlobsInclude.UncomittedBlobs);

            Assert.AreEqual(1, result.BlobList.Count);
        }

        [Test]
        public void ListBlobs_BlockAndPageBlobs_ReturnsBothTypes()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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

        #region PutBlockList

        [Test]
        public void PutBlockList_RequiredArgsOnly_CreatesBlockBlobFromLatestBlocks()
        {
            const string dataPerBlock = "foo";
            const string expectedData = "foofoofoo";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            AssertBlobContainsData(containerName, blobName, BlobType.BlockBlob, Encoding.Unicode.GetBytes(expectedData));
        }

        [Test]
        public async void PutBlockListAsync_RequiredArgsOnly_CreatesBlockBlobFromLatestBlocks()
        {
            const string dataPerBlock = "foo";
            const string expectedData = "foofoofoo";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            AssertBlobContainsData(containerName, blobName, BlobType.BlockBlob, Encoding.Unicode.GetBytes(expectedData));
        }

        [Test]
        public void PutBlockList_LeasedBlobCorrectLeaseSpecified_CreatesBlockBlobFromLatestBlocks()
        {
            const string dataPerBlock = "foo";
            const string expectedData = "foofoofoo";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            var lease = LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds, leaseId: lease);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            AssertBlobContainsData(containerName, blobName, BlobType.BlockBlob, Encoding.Unicode.GetBytes(expectedData));
        }

        [Test]
        public async void PutBlockListAsync_LeasedBlobCorrectLeaseSpecified_CreatesBlockBlobFromLatestBlocks()
        {
            const string dataPerBlock = "foo";
            const string expectedData = "foofoofoo";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            var lease = LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds, leaseId: lease);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            AssertBlobContainsData(containerName, blobName, BlobType.BlockBlob, Encoding.Unicode.GetBytes(expectedData));
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMissingAzureException))]
        public void PutBlockList_LeasedBlobWithNoLeaseGiven_ThrowsLeaseIdMissingAzureException()
        {
            const string dataPerBlock = "foo";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMissingAzureException))]
        public async void PutBlockListAsync_LeasedBlobWithNoLeaseGiven_ThrowsLeaseIdMissingAzureException()
        {
            const string dataPerBlock = "foo";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public void PutBlockList_LeasedBlobWithIncorrectLeaseGiven_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            const string dataPerBlock = "foo";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds, leaseId: GetGuidString());

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public async void PutBlockListAsync_LeasedBlobWithIncorrectLeaseGiven_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            const string dataPerBlock = "foo";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds, leaseId: GetGuidString());

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void PutBlockList_LeasedBlobWithInvalidLeaseGiven_ThrowsArgumentException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds, leaseId: InvalidLeaseId);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public async void PutBlockListAsync_LeasedBlobWithInvalidLeaseGiven_ThrowsArgumentException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds, leaseId: InvalidLeaseId);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(InvalidBlockListAzureException))]
        public void PutBlockList_InvalidBlockId_ThrowsInvalidBlockListAzureException()
        {
            const string dataPerBlock = "foo";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            blockListBlockIds.Add(new BlockListBlockId
            {
                Id = Base64Converter.ConvertToBase64("id4"),
                ListType = BlockListListType.Latest
            });
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds);

            // Throws exception
        }

        [Test]
        [ExpectedException(typeof(InvalidBlockListAzureException))]
        public async void PutBlockListAsync_InvalidBlockId_ThrowsInvalidBlockListAzureException()
        {
            const string dataPerBlock = "foo";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            blockListBlockIds.Add(new BlockListBlockId
            {
                Id = Base64Converter.ConvertToBase64("id4"),
                ListType = BlockListListType.Latest
            });
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds);

            // Throws exception
        }

        [Test]
        public void PutBlockList_WithMetadata_UploadsMetadata()
        {
            const string dataPerBlock = "foo";
            var expectedMetadata = new Dictionary<string, string>(){
                { "firstValue", "1" },
                { "secondValue", "2"}
            };
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds, metadata: expectedMetadata);

            var blob = AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.IsTrue(blob.Metadata.Any(m => m.Key == "firstValue" && m.Value == "1"), "First value is missing or incorrect");
            Assert.IsTrue(blob.Metadata.Any(m => m.Key == "secondValue" && m.Value == "2"), "Second value is missing or incorrect");
        }

        [Test]
        public async void PutBlockListAsync_WithMetadata_UploadsMetadata()
        {
            const string dataPerBlock = "foo";
            var expectedMetadata = new Dictionary<string, string>(){
                { "firstValue", "1" },
                { "secondValue", "2"}
            };
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds, metadata: expectedMetadata);

            var blob = AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.IsTrue(blob.Metadata.Any(m => m.Key == "firstValue" && m.Value == "1"), "First value is missing or incorrect");
            Assert.IsTrue(blob.Metadata.Any(m => m.Key == "secondValue" && m.Value == "2"), "Second value is missing or incorrect");
        }

        [Test]
        public void PutBlockList_WithContentType_UploadsWithSpecifiedContentType()
        {
            const string dataPerBlock = "foo";
            const string expectedContentType = "text/plain";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds, contentType: expectedContentType);

            var blob = AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentType, blob.Properties.ContentType);
        }

        [Test]
        public async void PutBlockListAsync_WithContentType_UploadsWithSpecifiedContentType()
        {
            const string dataPerBlock = "foo";
            const string expectedContentType = "text/plain";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds, contentType: expectedContentType);

            var blob = AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentType, blob.Properties.ContentType);
        }

        [Test]
        public void PutBlockList_WithBlobContentMD5_UploadsWithSpecifiedBlobContentMD5()
        {
            const string dataPerBlock = "foo";
            const string expectedData = "foofoofoo";
            var expectedContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(Encoding.Unicode.GetBytes(expectedData)));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds, blobContentMD5: expectedContentMD5);

            var blob = AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentMD5, blob.Properties.ContentMD5);
        }

        [Test]
        public async void PutBlockListAsync_WithBlobContentMD5_UploadsWithSpecifiedBlobContentMD5()
        {
            const string dataPerBlock = "foo";
            const string expectedData = "foofoofoo";
            var expectedContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(Encoding.Unicode.GetBytes(expectedData)));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds, blobContentMD5: expectedContentMD5);

            var blob = AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentMD5, blob.Properties.ContentMD5);
        }

        [Test]
        public void PutBlockList_WithBlobContentEncoding_UploadsWithSpecifiedBlobContentEncoding()
        {
            const string dataPerBlock = "foo";
            const string expectedContentEncoding = "UTF32";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock, Encoding.UTF32);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds, contentEncoding: expectedContentEncoding);

            var blob = AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentEncoding, blob.Properties.ContentEncoding);
        }

        [Test]
        public async void PutBlockListAsync_WithBlobContentEncoding_UploadsWithSpecifiedBlobContentEncoding()
        {
            const string dataPerBlock = "foo";
            const string expectedContentEncoding = "UTF32";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock, Encoding.UTF32);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds, contentEncoding: expectedContentEncoding);

            var blob = AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentEncoding, blob.Properties.ContentEncoding);
        }

        [Test]
        public void PutBlockList_WithBlobContentLanguage_UploadsWithSpecifiedBlobContentLanguage()
        {
            const string dataPerBlock = "foo";
            const string expectedContentLanguage = "gibberish";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds, contentLanguage: expectedContentLanguage);

            var blob = AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentLanguage, blob.Properties.ContentLanguage);
        }

        [Test]
        public async void PutBlockListAsync_WithBlobContentLanguage_UploadsWithSpecifiedBlobContentLanguage()
        {
            const string dataPerBlock = "foo";
            const string expectedContentLanguage = "gibberish";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var blockListBlockIds = CreateBlockIdList(3, BlockListListType.Latest);
            var blockIds = GetIdsFromBlockIdList(blockListBlockIds);
            CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds, contentLanguage: expectedContentLanguage);

            var blob = AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentLanguage, blob.Properties.ContentLanguage);
        }

        #endregion

        #region PutBlockBlob

        [Test]
        public void PutBlockBlob_RequiredArgsOnly_UploadsBlobSuccessfully()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            client.PutBlockBlob(containerName, blobName, data);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        public void PutBlockBlob_LeasedBlobWithCorrectLeaseIdSpecified_UploadsBlobSuccessfully()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var correctLease = LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            client.PutBlockBlob(containerName, blobName, data, leaseId: correctLease);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        public async void PutBlockBlobAsync_LeasedBlobWithCorrectLeaseId_UploadsBlobSuccessfully()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var correctLease = LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            await client.PutBlockBlobAsync(containerName, blobName, data, leaseId: correctLease);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMissingAzureException))]
        public void PutBlockBlob_LeasedBlobWithNoLeaseGiven_ThrowsLeaseIdMissingAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            client.PutBlockBlob(containerName, blobName, data);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMissingAzureException))]
        public async void PutBlockBlobAsync_LeasedBlobWithNoLeaseGiven_ThrowsLeaseIdMissingAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            await client.PutBlockBlobAsync(containerName, blobName, data);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public void PutBlockBlob_LeasedBlobWithIncorrectLeaseGiven_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            client.PutBlockBlob(containerName, blobName, data, leaseId: GetGuidString());

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public async void PutBlockBlobAsync_LeasedBlobWithIncorrectLeaseGiven_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            await client.PutBlockBlobAsync(containerName, blobName, data, leaseId: GetGuidString());

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void PutBlockBlob_LeasedBlobWithInvalidLeaseGiven_ThrowsArgumentException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            client.PutBlockBlob(containerName, blobName, data, leaseId: InvalidLeaseId);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public async void PutBlockBlobAsync_LeasedBlobWithInvalidLeaseGiven_ThrowsArgumentException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            await client.PutBlockBlobAsync(containerName, blobName, data, leaseId: InvalidLeaseId);

            // throws exception
        }

        [Test]
        public void PutBlockBlob_RequiredArgsOnlyAndBlobAlreadyExists_UploadsBlobSuccessfully()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            client.PutBlockBlob(containerName, blobName, data);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        public void PutBlockBlob_WithContentType_UploadsWithSpecifiedContentType()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");
            const string expectedContentType = "text/plain";

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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");
            const string expectedContentEncoding = "UTF8";

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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");
            const string expectedContentLanguage = "gibberish";

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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");
            var someOtherData = Encoding.UTF8.GetBytes("different content");
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");
            const string expectedCacheControl = "123-ABC";

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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");
            var someOtherData = Encoding.UTF8.GetBytes("different content");
            var incorrectContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(someOtherData));

            await client.PutBlockBlobAsync(containerName, blobName, data, contentMD5: incorrectContentMD5);

            // expects exception
        }

        #endregion

        #region PutBlock

        [Test]
        public void PutBlock_RequiredArgsOnly_UploadsBlockSuccessfully()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var data = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlock(containerName, blobName, blockId, data);

            AssertBlockExists(containerName, blobName, blockId);
        }

        [Test]
        public async void PutBlockAsync_RequiredArgsOnly_UploadsBlockSuccessfully()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var data = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockAsync(containerName, blobName, blockId, data);

            AssertBlockExists(containerName, blobName, blockId);
        }

        [Test]
        public void PutBlock_LeasedBlobCorrectLeaseSpecified_UploadsBlockSuccessfully()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockData = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var lease = LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlock(containerName, blobName, blockId, blockData, leaseId: lease);

            AssertBlockExists(containerName, blobName, blockId);
        }

        [Test]
        public async void PutBlockAsync_LeasedBlobCorrectLeaseSpecified_UploadsBlockSuccessfully()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockData = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var lease = LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockAsync(containerName, blobName, blockId, blockData, leaseId: lease);

            AssertBlockExists(containerName, blobName, blockId);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMissingAzureException))]
        public void PutBlock_LeasedBlobWithNoLeaseGiven_ThrowsLeaseIdMissingAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockData = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlock(containerName, blobName, blockId, blockData);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMissingAzureException))]
        public async void PutBlockAsync_LeasedBlobWithNoLeaseGiven_ThrowsLeaseIdMissingAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockData = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockAsync(containerName, blobName, blockId, blockData);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public void PutBlock_LeasedBlobWithIncorrectLeaseGiven_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockData = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlock(containerName, blobName, blockId, blockData, leaseId: GetGuidString());

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public async void PutBlockAsync_LeasedBlobWithIncorrectLeaseGiven_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockData = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockAsync(containerName, blobName, blockId, blockData, leaseId: GetGuidString());

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void PutBlock_LeasedBlobWithIncorrectLeaseGiven_ThrowsArgumentException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockData = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlock(containerName, blobName, blockId, blockData, leaseId: InvalidLeaseId);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public async void PutBlockAsync_LeasedBlobWithIncorrectLeaseGiven_ThrowsArgumentException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockData = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockAsync(containerName, blobName, blockId, blockData, leaseId: InvalidLeaseId);

            // throws exception
        }

        [Test]
        public void PutBlock_WithContentMD5_UploadsWithSpecifiedContentMD5()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var data = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var expectedContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(data));
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var block = client.PutBlock(containerName, blobName, blockId, data, contentMD5: expectedContentMD5);

            AssertBlockExists(containerName, blobName, blockId);
            Assert.AreEqual(expectedContentMD5, block.ContentMD5);
        }

        [Test]
        public async void PutBlockAsync_WithContentMD5_UploadsWithSpecifiedContentMD5()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var data = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var expectedContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(data));
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var block = await client.PutBlockAsync(containerName, blobName, blockId, data, contentMD5: expectedContentMD5);

            AssertBlockExists(containerName, blobName, blockId);
            Assert.AreEqual(expectedContentMD5, block.ContentMD5);
        }

        [Test]
        [ExpectedException(typeof(Md5MismatchAzureException))]
        public void PutBlock_WithIncorrectContentMD5_ThrowsMD5MismatchAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var data = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var someOtherData = Encoding.UTF8.GetBytes("different content");
            var incorrectContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(someOtherData));
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlock(containerName, blobName, blockId, data, contentMD5: incorrectContentMD5);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(Md5MismatchAzureException))]
        public async void PutBlockAsync_WithIncorrectContentMD5_ThrowsMD5MismatchAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var data = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var someOtherData = Encoding.UTF8.GetBytes("different content");
            var incorrectContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(someOtherData));
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockAsync(containerName, blobName, blockId, data, contentMD5: incorrectContentMD5);

            // expects exception
        }

        [Test]
        public void PutBlock_RequiredArgsOnly_ReturnsCorrectMD5Hash()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");
            var expectedContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(data));

            var response = client.PutBlock(containerName, blobName, blockId, data);

            Assert.AreEqual(expectedContentMD5, response.ContentMD5);
        }

        [Test]
        public async void PutBlockAsync_RequiredArgsOnly_ReturnsCorrectMD5Hash()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");
            var expectedContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(data));

            var response = await client.PutBlockAsync(containerName, blobName, blockId, data);

            Assert.AreEqual(expectedContentMD5, response.ContentMD5);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void PutBlock_TooLargePayload_ThrowsArgumentException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var fiveMegabytes = new byte[5242880];
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlock(containerName, blobName, blockId, fiveMegabytes);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public async void PutBlockAsync_TooLargePayload_ThrowsArgumentException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var fiveMegabytes = new byte[5242880];
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockAsync(containerName, blobName, blockId, fiveMegabytes);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(InvalidBlobOrBlockAzureException))]
        public void PutBlock_DifferentLengthBlockIds_ThrowsInvalidBlobOrBlockAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var differentLengthBlockId = Base64Converter.ConvertToBase64("test-block-id-wrong-length");
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            client.PutBlock(containerName, blobName, blockId, data);
            client.PutBlock(containerName, blobName, differentLengthBlockId, data);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(InvalidBlobOrBlockAzureException))]
        public async void PutBlockAsync_DifferentLengthBlockIds_ThrowsInvalidBlobOrBlockAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var differentLengthBlockId = Base64Converter.ConvertToBase64("test-block-id-wrong-length");
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            await client.PutBlockAsync(containerName, blobName, blockId, data);
            await client.PutBlockAsync(containerName, blobName, differentLengthBlockId, data);

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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            client.PutBlock(containerName, blobName, blockId, data);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public async void PutBlockAsync_BlockIdTooLarge_ThrowsArgumentException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var blockId = Base64Converter.ConvertToBase64("test-block-id-very-long-too-long-horribly-wrong-does-not-compute-danger-will-robinson");
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            await client.PutBlockAsync(containerName, blobName, blockId, data);

            // throws exception
        }

        #endregion

        #region PutPageBlob

        [Test]
        public void PutPageBlob_WithRequiredArgs_CreatesNewPageBlob()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var expectedMetadata = new Dictionary<string, string>(){
                { "firstValue", "1" },
                { "secondValue", "2"}
            };
            const int expectedSize = 512;

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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            const int expectedSize = 512;
            const long expectedSequenceNumber = 123;

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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            int expectedSize = 512;

            await client.PutPageBlobAsync(containerName, blobName, expectedSize);

            var blob = AssertBlobExists(containerName, blobName, BlobType.PageBlob);
            Assert.AreEqual(expectedSize, blob.Properties.Length);
        }

        #endregion

        #region DeleteBlob

        [Test]
        public void DeleteBlob_ExistingBlob_DeletesBlob()
        {
            var expectedContent = "Expected blob content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName, content: expectedContent);
            var client = new BlobServiceClient(AccountSettings);

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
            var client = new BlobServiceClient(AccountSettings);

            await client.DeleteBlobAsync(containerName, blobName);

            AssertBlobDoesNotExist(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        public void DeleteBlob_LeasedBlobCorrectLeaseSpecified_DeletesBlob()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var lease = LeaseBlob(containerName, blobName);
            var client = new BlobServiceClient(AccountSettings);

            client.DeleteBlob(containerName, blobName, leaseId: lease);

            AssertBlobDoesNotExist(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        public async void DeleteBlobAsync_LeasedBlobCorrectLeaseSpecified_DeletesBlob()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var lease = LeaseBlob(containerName, blobName);
            var client = new BlobServiceClient(AccountSettings);

            await client.DeleteBlobAsync(containerName, blobName, leaseId: lease);

            AssertBlobDoesNotExist(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMissingAzureException))]
        public void DeleteBlob_LeasedBlobWithNoLeaseGiven_ThrowsLeaseIdMissingAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);
            var client = new BlobServiceClient(AccountSettings);

            client.DeleteBlob(containerName, blobName);

            // throw exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMissingAzureException))]
        public async void DeleteBlobAsync_LeasedBlobWithNoLeaseGiven_ThrowsLeaseIdMissingAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);
            var client = new BlobServiceClient(AccountSettings);

            await client.DeleteBlobAsync(containerName, blobName);

            // throw exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public void DeleteBlob_LeasedBlobWithIncorrectLeaseGiven_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);
            var client = new BlobServiceClient(AccountSettings);

            client.DeleteBlob(containerName, blobName, leaseId: GetGuidString());

            // throw exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public async void DeleteBlobAsync_LeasedBlobWithIncorrectLeaseGiven_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);
            var client = new BlobServiceClient(AccountSettings);

            await client.DeleteBlobAsync(containerName, blobName, leaseId: GetGuidString());

            // throw exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void DeleteBlob_LeasedBlobWithInvalidLeaseGiven_ThrowsArgumentException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var client = new BlobServiceClient(AccountSettings);

            client.DeleteBlob(containerName, blobName, leaseId: InvalidLeaseId);

            // throw exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public async void DeleteBlobAsync_LeasedBlobWithInvalidLeaseGiven_ThrowsArgumentException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            var client = new BlobServiceClient(AccountSettings);

            await client.DeleteBlobAsync(containerName, blobName, leaseId: InvalidLeaseId);

            // throw exception
        }

        [Test]
        [ExpectedException(typeof(BlobNotFoundAzureException))]
        public void DeleteBlob_NonExistingBlob_ThrowsBlobDoesNotExistException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(AccountSettings);

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
            var client = new BlobServiceClient(AccountSettings);

            // delete blog that doesn't exist => should throw an exception
            await client.DeleteBlobAsync(containerName, blobName);
        }

        #endregion

        #region GetBlob

        [Test]
        public void GetBlob_ExistingBlob_DownloadsBlobBytes()
        {
            const string expectedContent = "Expected blob content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlob(containerName, blobName);
            var data = response.GetDataBytes();

            Assert.AreEqual(expectedContent, Encoding.UTF8.GetString(data));
        }

        [Test]
        public async Task GetBlobAsync_ExistingBlob_DownloadsBlobBytes()
        {
            const string expectedContent = "Expected blob content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = await client.GetBlobAsync(containerName, blobName);
            var data = response.GetDataBytes();

            Assert.AreEqual(expectedContent, Encoding.UTF8.GetString(data));
        }

        [Test]
        public void GetBlob_LeasedBlobWithCorrectLeaseSpecified_GetsBlobWithoutException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var leaseId = LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.GetBlob(containerName, blobName, null, leaseId);

            // no exception thrown
        }

        [Test]
        public async void GetBlobAsync_LeasedBlobWithCorrectLeaseSpecified_GetsBlobWithoutException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var leaseId = LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.GetBlobAsync(containerName, blobName, null, leaseId);

            // no exception thrown
        }

        [Test]
        public void GetBlob_LeasedBlobWithoutLeaseSpecified_GetsBlobWithoutException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var blob = client.GetBlob(containerName, blobName);

            blob = null;
            client = null;

            // no exception thrown
        }

        [Test]
        public async void GetBlobAsync_LeasedBlobWithoutLeaseSpecified_GetsBlobWithoutException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.GetBlobAsync(containerName, blobName);

            // no exception thrown
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public void GetBlob_LeasedBlobGivenIncorrectLease_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.GetBlob(containerName, blobName, null, leaseId: GetGuidString());

            // Throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public async void GetBlobAsync_LeasedBlobGivenIncorrectLease_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.GetBlobAsync(containerName, blobName, null, leaseId: GetGuidString());

            // Throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void GetBlob_LeasedBlobGivenInvalidLease_ThrowsArgumentException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.GetBlob(containerName, blobName, null, leaseId: InvalidLeaseId);

            // Throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public async void GetBlobAsync_LeasedBlobGivenInvalidLease_ThrowsArgumentException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.GetBlobAsync(containerName, blobName, null, leaseId: InvalidLeaseId);

            // Throws exception
        }

        [Test]
        public void GetBlob_ExistingBlob_DownloadsBlobStream()
        {
            const string expectedContent = "Expected blob content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlob(containerName, blobName);
            byte[] data;
            using (var stream = response.GetDataStream())
            {
                using (var ms = new MemoryStream())
                {
                    stream.CopyTo(ms);
                    data = ms.ToArray();
                }
            }

            Assert.AreEqual(expectedContent, Encoding.UTF8.GetString(data));
        }

        [Test]
        public async Task GetBlobAsync_ExistingBlob_DownloadsBlobStream()
        {
            const string expectedContent = "Expected blob content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = await client.GetBlobAsync(containerName, blobName);
            byte[] data;
            using (var stream = response.GetDataStream())
            {
                using (var ms = new MemoryStream())
                {
                    stream.CopyTo(ms);
                    data = ms.ToArray();
                }
            }

            Assert.AreEqual(expectedContent, Encoding.UTF8.GetString(data));
        }

        [Test]
        [ExpectedException(typeof(BlobNotFoundAzureException))]
        public void GetBlob_NonExistentBlob_ThrowsBlobDoesNotExistException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.GetBlob(containerName, blobName);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(BlobNotFoundAzureException))]
        public async Task GetBlobAsync_NonExistentBlob_ThrowsBlobDoesNotExistException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.GetBlobAsync(containerName, blobName);

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
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlob(containerName, blobName, range: new BlobRange(2));
            var data = response.GetDataBytes();

            Assert.AreEqual(expectedContent.Substring(2), Encoding.UTF8.GetString(data));
        }

        [Test]
        public async void GetBlobAsync_ExistingBlobByValidStartRange_DownloadBlobRangeOnly()
        {
            var expectedContent = "Expected blob content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = await client.GetBlobAsync(containerName, blobName, range: new BlobRange(2));
            var data = response.GetDataBytes();

            Assert.AreEqual(expectedContent.Substring(2), Encoding.UTF8.GetString(data));
        }

        [Test]
        public void GetBlob_ExistingBlobByValidStartAndEndRange_DownloadBlobRangeOnly()
        {
            var expectedContent = "Expected blob content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlob(containerName, blobName, range: new BlobRange(2, 6));
            var data = response.GetDataBytes();

            Assert.AreEqual(expectedContent.Substring(2, 5), Encoding.UTF8.GetString(data));
        }

        [Test]
        public async void GetBlobAsync_ExistingBlobByValidStartAndEndRange_DownloadBlobRangeOnly()
        {
            var expectedContent = "Expected blob content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = await client.GetBlobAsync(containerName, blobName, range: new BlobRange(2, 6));
            var data = response.GetDataBytes();

            Assert.AreEqual(expectedContent.Substring(2, 5), Encoding.UTF8.GetString(data));
        }

        #endregion

        #region LeaseBlob

        [Test]
        public void LeaseBlobAcquire_AcquireLeaseForValidBlob_AcquiresLease()
        {
            const int leaseDuration = 30;
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);

            var response = client.LeaseBlobAcquire(containerName, blobName, leaseDuration);

            AssertBlobIsLeased(containerName, blobName, response.LeaseId);
        }

        [Test]
        public async void LeaseBlobAcquireAsync_AcquireLeaseForValidBlob_AcquiresLease()
        {
            const int leaseDuration = 30;
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);

            var response = await client.LeaseBlobAcquireAsync(containerName, blobName, leaseDuration);

            AssertBlobIsLeased(containerName, blobName, response.LeaseId);
        }

        [Test]
        public void LeaseBlobAcquire_AcquireInfiniteLeaseForValidBlob_AcquiresLease()
        {
            const int infiniteLease = -1;
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);

            var response = client.LeaseBlobAcquire(containerName, blobName, infiniteLease);

            AssertBlobIsLeased(containerName, blobName, response.LeaseId);
        }

        [Test]
        public async void LeaseBlobAcquireAsync_AcquireInfiniteLeaseForValidBlob_AcquiresLease()
        {
            const int infiniteLease = -1;
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);

            var response = await client.LeaseBlobAcquireAsync(containerName, blobName, infiniteLease);

            AssertBlobIsLeased(containerName, blobName, response.LeaseId);
        }

        [Test]
        public void LeaseBlobAcquire_AcquireSpecificLeaseIdForValidBlob_AcquiresLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            var response = client.LeaseBlobAcquire(containerName, blobName, 30, expectedLeaseId);

            Assert.AreEqual(expectedLeaseId, response.LeaseId);
            AssertBlobIsLeased(containerName, blobName, response.LeaseId);
        }

        [Test]
        public async void LeaseBlobAcquireAsync_AcquireSpecificLeaseIdForValidBlob_AcquiresLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            var response = await client.LeaseBlobAcquireAsync(containerName, blobName, 30, expectedLeaseId);

            Assert.AreEqual(expectedLeaseId, response.LeaseId);
            AssertBlobIsLeased(containerName, blobName, response.LeaseId);
        }

        [Test]
        [ExpectedException(typeof(BlobNotFoundAzureException))]
        public void LeaseBlobAcquire_InvalidBlob_ThrowsBlobNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            var blobName = GenerateSampleBlobName();

            client.LeaseBlobAcquire(containerName, blobName);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(BlobNotFoundAzureException))]
        public async void LeaseBlobAcquireAsync_InvalidBlob_ThrowsBlobNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            var blobName = GenerateSampleBlobName();

            await client.LeaseBlobAcquireAsync(containerName, blobName);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(LeaseAlreadyPresentAzureException))]
        public void LeaseBlobAcquire_AlreadyLeasedBlob_ThrowsAlreadyPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);

            client.LeaseBlobAcquire(containerName, blobName);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(LeaseAlreadyPresentAzureException))]
        public async void LeaseBlobAcquireAsync_AlreadyLeasedBlob_ThrowsAlreadyPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            LeaseBlob(containerName, blobName);

            await client.LeaseBlobAcquireAsync(containerName, blobName);

            // expects exception
        }

        [Test]
        public void LeaseBlobRenew_LeasedBlob_RenewsActiveLease()
        {
            var minimumWaitTime = TimeSpan.FromSeconds(15);
            var halfOfMinimum = minimumWaitTime.Subtract(TimeSpan.FromSeconds(minimumWaitTime.TotalSeconds*0.5));
            var threeQuartersOfMinimum = minimumWaitTime.Subtract(TimeSpan.FromSeconds(minimumWaitTime.TotalSeconds*0.75));
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var leaseId = LeaseBlob(containerName, blobName, TimeSpan.FromSeconds(15));
            Thread.Sleep(halfOfMinimum);

            client.LeaseBlobRenew(containerName, blobName, leaseId);

            Thread.Sleep(threeQuartersOfMinimum); // wait again... if it didn't renew, by now it would be expired
            AssertBlobIsLeased(containerName, blobName, leaseId);
        }

        [Test]
        public async void LeaseBlobRenewAsync_LeasedBlob_RenewsActiveLease()
        {
            var minimumWaitTime = TimeSpan.FromSeconds(15);
            var halfOfMinimum = minimumWaitTime.Subtract(TimeSpan.FromSeconds(minimumWaitTime.TotalSeconds * 0.5));
            var threeQuartersOfMinimum = minimumWaitTime.Subtract(TimeSpan.FromSeconds(minimumWaitTime.TotalSeconds * 0.75));
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var leaseId = LeaseBlob(containerName, blobName, TimeSpan.FromSeconds(15));
            Thread.Sleep(halfOfMinimum);

            await client.LeaseBlobRenewAsync(containerName, blobName, leaseId);

            Thread.Sleep(threeQuartersOfMinimum); // wait again... if it didn't renew, by now it would be expired
            AssertBlobIsLeased(containerName, blobName, leaseId);
        }

        [Test]
        public void LeaseBlobRenew_RecentlyLeasedBlob_RenewsLease()
        {
            var minimumWaitTime = TimeSpan.FromSeconds(15);
            var moreThanMinimumWaitTime = minimumWaitTime.Add(TimeSpan.FromSeconds(1));
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var leaseId = LeaseBlob(containerName, blobName, TimeSpan.FromSeconds(15));
            Thread.Sleep(moreThanMinimumWaitTime);

            client.LeaseBlobRenew(containerName, blobName, leaseId);
            
            AssertBlobIsLeased(containerName, blobName, leaseId);
        }

        [Test]
        public async void LeaseBlobRenewAsync_RecentlyLeasedBlob_RenewsLease()
        {
            var minimumWaitTime = TimeSpan.FromSeconds(15);
            var moreThanMinimumWaitTime = minimumWaitTime.Add(TimeSpan.FromSeconds(1));
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var leaseId = LeaseBlob(containerName, blobName, TimeSpan.FromSeconds(15));
            Thread.Sleep(moreThanMinimumWaitTime);

            await client.LeaseBlobRenewAsync(containerName, blobName, leaseId);

            AssertBlobIsLeased(containerName, blobName, leaseId);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithLeaseOperationAzureException))]
        public void LeaseBlobRenew_NonLeasedBlob_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);

            client.LeaseBlobRenew(containerName, blobName, FakeLeaseId);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithLeaseOperationAzureException))]
        public async void LeaseBlobRenewAsync_NonLeasedBlob_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);

            await client.LeaseBlobRenewAsync(containerName, blobName, FakeLeaseId);

            // expects exception
        }

        [Test]
        public void LeaseBlobChange_LeasedBlobToNewLeaseId_ChangesToMatchNewLeaseId()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var leaseId = LeaseBlob(containerName, blobName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            var response = client.LeaseBlobChange(containerName, blobName, leaseId, expectedLeaseId);

            Assert.AreEqual(expectedLeaseId, response.LeaseId);
            AssertBlobIsLeased(containerName, blobName, expectedLeaseId);
        }

        [Test]
        public async void LeaseBlobChangeAsync_LeasedBlobToNewLeaseId_ChangesToMatchNewLeaseId()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var leaseId = LeaseBlob(containerName, blobName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            var response = await client.LeaseBlobChangeAsync(containerName, blobName, leaseId, expectedLeaseId);

            Assert.AreEqual(expectedLeaseId, response.LeaseId);
            AssertBlobIsLeased(containerName, blobName, expectedLeaseId);
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithLeaseOperationAzureException))]
        public void LeaseBlobChange_NonLeasedBlob_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            client.LeaseBlobChange(containerName, blobName, FakeLeaseId, expectedLeaseId);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithLeaseOperationAzureException))]
        public async void LeaseBlobChangeAsync_NonLeasedBlob_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            await client.LeaseBlobChangeAsync(containerName, blobName, FakeLeaseId, expectedLeaseId);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(BlobNotFoundAzureException))]
        public void LeaseBlobChange_NonexistentBlob_ThrowsBlobNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            client.LeaseBlobChange(containerName, blobName, FakeLeaseId, expectedLeaseId);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(BlobNotFoundAzureException))]
        public async void LeaseBlobChangeAsync_NonexistentBlob_ThrowsBlobNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            await client.LeaseBlobChangeAsync(containerName, blobName, FakeLeaseId, expectedLeaseId);

            // expects exception
        }

        [Test]
        public void LeaseBlobRelease_LeasedBlob_ReleasesLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var leaseId = LeaseBlob(containerName, blobName);

            client.LeaseBlobRelease(containerName, blobName, leaseId);

            AssertBlobIsNotLeased(containerName, blobName);
        }

        [Test]
        public async void LeaseBlobReleaseAsync_LeasedBlob_ReleasesLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var leaseId = LeaseBlob(containerName, blobName);

            await client.LeaseBlobReleaseAsync(containerName, blobName, leaseId);

            AssertBlobIsNotLeased(containerName, blobName);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithLeaseOperationAzureException))]
        public void LeaseBlobRelease_NonLeasedBlob_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);

            client.LeaseBlobRelease(containerName, blobName, FakeLeaseId);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithLeaseOperationAzureException))]
        public async void LeaseBlobReleaseAsync_NonLeasedBlob_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);

            await client.LeaseBlobReleaseAsync(containerName, blobName, FakeLeaseId);

            // expects exception
        }

        [Test]
        public void LeaseBlobBreak_LeasedBlob_BreaksLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var leaseId = LeaseBlob(containerName, blobName);

            client.LeaseBlobBreak(containerName, blobName, leaseId, 0);

            var leaseState = GetBlobLeaseState(containerName, blobName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.LeaseState.Broken, leaseState);
        }

        [Test]
        public async void LeaseBlobBreakAsync_LeasedBlob_BreaksLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var leaseId = LeaseBlob(containerName, blobName);

            await client.LeaseBlobBreakAsync(containerName, blobName, leaseId, 0);

            var leaseState = GetBlobLeaseState(containerName, blobName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.LeaseState.Broken, leaseState);
        }

        [Test]
        public void LeaseBlobBreak_LeasedBlobWithLongBreakPeriod_SetLeaseToBreaking()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var leaseId = LeaseBlob(containerName, blobName);

            client.LeaseBlobBreak(containerName, blobName, leaseId, 60);

            var leaseState = GetBlobLeaseState(containerName, blobName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.LeaseState.Breaking, leaseState);
        }

        [Test]
        public async void LeaseBlobBreakAsync_LeasedBlobWithLongBreakPeriod_SetLeaseToBreaking()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);
            var leaseId = LeaseBlob(containerName, blobName);

            await client.LeaseBlobBreakAsync(containerName, blobName, leaseId, 60);

            var leaseState = GetBlobLeaseState(containerName, blobName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.LeaseState.Breaking, leaseState);
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithLeaseOperationAzureException))]
        public void LeaseBlobBreak_NonLeasedBlob_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);

            client.LeaseBlobBreak(containerName, blobName, FakeLeaseId, 0);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithLeaseOperationAzureException))]
        public async void LeaseBlobBreakAsync_NonLeasedBlob_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlockBlob(containerName, blobName);

            await client.LeaseBlobBreakAsync(containerName, blobName, FakeLeaseId, 0);

            // expects exception
        }
        
        #endregion

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

    }
}
