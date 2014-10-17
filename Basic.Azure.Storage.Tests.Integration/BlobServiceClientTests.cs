using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Microsoft.WindowsAzure.Storage;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Tests.Integration
{
    [TestFixture]
    public class BlobServiceClientTests
    {

        private StorageAccountSettings _accountSettings = new LocalEmulatorAccountSettings();
        private CloudStorageAccount _storageAccount = CloudStorageAccount.Parse("UseDevelopmentStorage=true");

        private Dictionary<string, string> _containersToCleanUp = new Dictionary<string,string>();

        private string GenerateSampleContainerName()
        {
            var name = "unit-test-" + Guid.NewGuid().ToString().ToLower();
            _containersToCleanUp.Add(name, null);
            return name;
        }

        private string GenerateSampleBlobName()
        {
            return String.Format("unit-test-{0}", Guid.NewGuid());
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
                    try {
                        container.ReleaseLease(new AccessCondition() { LeaseId = containerPair.Value });
                    }
                    catch { }
                }
                container.DeleteIfExists();
            }
        }

        #region Container Operations Tests

        [Test]
        public void CreateContainer_ValidArguments_CreatesContainerWithSpecificName()
        {
            var containerName = GenerateSampleContainerName();
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.None);

            AssertContainerExists(containerName);
        }

        [Test]
        public void CreateContainer_ValidArgumentsWithPublicContainerAccess_CreatesContainerWithContainerAccess()
        {
            var containerName = GenerateSampleContainerName();
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.PublicContainer);

            AssertContainerAccess(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Container);
        }

        [Test]
        public void CreateContainer_ValidArgumentsWithPublicBlobAccess_CreatesContainerWithBlobAccess()
        {
            var containerName = GenerateSampleContainerName();
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.PublicBlob);

            AssertContainerAccess(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Blob);
        }

        [Test]
        public void CreateContainer_ValidArgumentsWithNoPublicAccess_CreatesContainerWithNoPublicAccess()
        {
            var containerName = GenerateSampleContainerName();
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.None);

            AssertContainerAccess(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Off);
        }

        [Test]
        [ExpectedException(typeof(ContainerAlreadyExistsAzureException))]
        public void CreateContainer_AlreadyExists_ThrowsContainerAlreadyExistsException()
        {
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.None);

            // expects exception
        }

        [Test]
        public void CreateContainer_ValidArguments_ReturnsContainerCreationResponse()
        {
            var containerName = GenerateSampleContainerName();
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);

            var response = client.CreateContainer(containerName, ContainerAccessType.None);

            Assert.IsTrue(response.Date > DateTime.Now.AddSeconds(-5), String.Format("Response Date was set to {0}", response.Date));
            Assert.IsTrue(response.LastModified > DateTime.Now.AddSeconds(-5), String.Format("Response LastModified was set to {0}", response.LastModified));
            Assert.IsFalse(string.IsNullOrWhiteSpace(response.ETag), "Response ETag is not set");

        }
        
        [Test]
        public async Task CreateContainerAsync_ValidArguments_CreatesContainerWithSpecificName()
        {
            var containerName = GenerateSampleContainerName();
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);

            await client.CreateContainerAsync(containerName, ContainerAccessType.None);

            AssertContainerExists(containerName);
        }

        [Test]
        [ExpectedException(typeof(ContainerAlreadyExistsAzureException))]
        public async Task CreateContainerAsync_AlreadyExists_ThrowsContainerAlreadyExistsException()
        {
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);

            await client.CreateContainerAsync(containerName, ContainerAccessType.None);

            // expects exception
        }

        [Test]
        public void GetContainerProperties_ValidContainer_ReturnsProperties()
        {
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
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
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();

            var response = client.GetContainerProperties(containerName);

            // expects exception
        }

        [Test]
        public void GetContainerProperties_ContainerWithMetadata_ReturnsMetadata()
        {
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
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
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
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
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
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
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
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
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
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
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();

            var response = await client.GetContainerPropertiesAsync(containerName);

            // expects exception
        }

        [Test]
        public void GetContainerMetadata_ValidContainer_ReturnsMetadata()
        {
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
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
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
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
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();

            client.GetContainerMetadata(containerName);

            //expects exception
        }

        [Test]
        public async Task GetContainerMetadataAsync_ValidContainer_ReturnsMetadata()
        {
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
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
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();

            await client.GetContainerMetadataAsync(containerName);

            //expects exception
        }

        [Test]
        public void SetContainerMetadata_ValidContainer_SetsMetadataOnContainer()
        {
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
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
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
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
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
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
        public void SetContainerMetadata_NonLeasedContainerWithLease_ThrowsPreconditionFailureException()
        {
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            client.SetContainerMetadata(containerName, new Dictionary<string, string>() { 
                { "a", "1"},
                { "b", "2"}
            }, FakeLeaseId);

            // expects exception
        }

        [Test]
        public async Task SetContainerMetadataAsync_ValidContainer_SetsMetadataOnContainer()
        {
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
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
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
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
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);

            var result = client.GetContainerACL(containerName);

            Assert.IsEmpty(result.SignedIdentifiers);
        }

        [Test]
        public void GetContainerACL_HasAccessPolicies_ReturnsListConstainingThosePolicies()
        {
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
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
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            AddContainerAccessPolicy(containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Container);

            var result = client.GetContainerACL(containerName);

            Assert.AreEqual(ContainerAccessType.PublicContainer, result.PublicAccess);
        }

        [Test]
        public void GetContainerACL_NoPublicAccess_ReturnsPublicAccessAsNone()
        {
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
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
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();

            var result = client.GetContainerACL(containerName);

            // expects exception
        }


        [Test]
        public async Task GetContainerACLAsync_HasAccessPolicies_ReturnsListConstainingThosePolicies()
        {
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
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
            IBlobStorageClient client = new BlobServiceClient(_accountSettings);
            var containerName = GenerateSampleContainerName();

            var result = await client.GetContainerACLAsync(containerName);

            // expects exception
        }

        #endregion

        #region Blob Operation Tests

        [Test]
        public void PutBlockBlob_RequiredArgsOnly_UploadsBlobSuccessfully()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");

            client.PutBlockBlob(containerName, blobName, data);

            AssertBlobExists(containerName, blobName);
        }

        [Test]
        public void PutBlockBlob_RequiredArgsOnlyAndBlobAlreadyExists_UploadsBlobSuccessfully()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            CreateBlob(containerName, blobName);
            var client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");

            client.PutBlockBlob(containerName, blobName, data);

            AssertBlobExists(containerName, blobName);
        }

        [Test]
        public void PutBlockBlob_WithContentType_UploadsWithSpecifiedContentType()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            string expectedContentType = "text/plain";

            client.PutBlockBlob(containerName, blobName, data, contentType: expectedContentType);

            var blob = AssertBlobExists(containerName, blobName);
            Assert.AreEqual(expectedContentType, blob.Properties.ContentType);
        }

        [Test]
        public void PutBlockBlob_WithContentEncoding_UploadsWithSpecifiedContentEncoding()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            string expectedContentEncoding = "UTF8";

            client.PutBlockBlob(containerName, blobName, data, contentEncoding: expectedContentEncoding);

            var blob = AssertBlobExists(containerName, blobName);
            Assert.AreEqual(expectedContentEncoding, blob.Properties.ContentEncoding);
        }

        [Test]
        public void PutBlockBlob_WithContentLanguage_UploadsWithSpecifiedContentLanguage()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            string expectedContentLanguage = "gibberish";

            client.PutBlockBlob(containerName, blobName, data, contentLanguage: expectedContentLanguage);

            var blob = AssertBlobExists(containerName, blobName);
            Assert.AreEqual(expectedContentLanguage, blob.Properties.ContentLanguage);
        }

        [Test]
        public void PutBlockBlob_WithContentMD5_UploadsWithSpecifiedContentMD5()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            var expectedContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(data));

            client.PutBlockBlob(containerName, blobName, data, contentMD5: expectedContentMD5);

            var blob = AssertBlobExists(containerName, blobName);
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
            var client = new BlobServiceClient(_accountSettings);
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
            var client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            string expectedCacheControl = "123-ABC";

            client.PutBlockBlob(containerName, blobName, data, cacheControl: expectedCacheControl);

            var blob = AssertBlobExists(containerName, blobName);
            Assert.AreEqual(expectedCacheControl, blob.Properties.CacheControl);
        }

        [Test]
        public void PutBlockBlob_WithMetadata_UploadsMetadata()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            var expectedMetadata = new Dictionary<string, string>(){
                { "firstValue", "1" },
                { "secondValue", "2"}
            };

            client.PutBlockBlob(containerName, blobName, data, metadata: expectedMetadata);

            var blob = AssertBlobExists(containerName, blobName);
            Assert.IsTrue(blob.Metadata.Any(m => m.Key == "firstValue" && m.Value == "1"), "First value is missing or incorrect");
            Assert.IsTrue(blob.Metadata.Any(m => m.Key == "secondValue" && m.Value == "2"), "Second value is missing or incorrect");
        }


        [Test]
        public async Task PutBlockBloAsyncb_RequiredArgsOnly_UploadsBlobSuccessfully()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");

            await client.PutBlockBlobAsync(containerName, blobName, data);

            AssertBlobExists(containerName, blobName);
        }

        [Test]
        [ExpectedException(typeof(Md5MismatchAzureException))]
        public async Task PutBlockBlobAsync_WithIncorrectContentMD5_ThrowsMD5MismatchAzureException()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            var someOtherData = UTF8Encoding.UTF8.GetBytes("different content");
            var incorrectContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(someOtherData));

            await client.PutBlockBlobAsync(containerName, blobName, data, contentMD5: incorrectContentMD5);

            // expects exception
        }

        [Test]
        public void PutPageBlob_WithRequiredArgs_CreatesNewPageBlob()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);
            int expectedSize = 512;

            client.PutPageBlob(containerName, blobName, expectedSize);

            var blob = AssertBlobExists(containerName, blobName);
            Assert.AreEqual(expectedSize, blob.Properties.Length);
        }

        [Test]
        public void PutPageBlob_WithContentType_UploadsWithSpecifiedContentType()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);
            string expectedContentType = "text/plain";
            int expectedSize = 512;

            client.PutPageBlob(containerName, blobName, expectedSize, contentType: expectedContentType);

            var blob = AssertBlobExists(containerName, blobName);
            Assert.AreEqual(expectedContentType, blob.Properties.ContentType);
        }

        [Test]
        public void PutPageBlob_WithContentEncoding_UploadsWithSpecifiedContentEncoding()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);
            string expectedContentEncoding = "UTF8";
            int expectedSize = 512;

            client.PutPageBlob(containerName, blobName, expectedSize, contentEncoding: expectedContentEncoding);

            var blob = AssertBlobExists(containerName, blobName);
            Assert.AreEqual(expectedContentEncoding, blob.Properties.ContentEncoding);
        }

        [Test]
        public void PutPageBlob_WithContentLanguage_UploadsWithSpecifiedContentLanguage()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);
            string expectedContentLanguage = "gibberish";
            int expectedSize = 512;

            client.PutPageBlob(containerName, blobName, expectedSize, contentLanguage: expectedContentLanguage);

            var blob = AssertBlobExists(containerName, blobName);
            Assert.AreEqual(expectedContentLanguage, blob.Properties.ContentLanguage);
        }

        [Test]
        public void PutPageBlob_WithCacheControl_UploadsWithCacheControl()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);
            string expectedCacheControl = "123-ABC";
            int expectedSize = 512;

            client.PutPageBlob(containerName, blobName, expectedSize, cacheControl: expectedCacheControl);

            var blob = AssertBlobExists(containerName, blobName);
            Assert.AreEqual(expectedCacheControl, blob.Properties.CacheControl);
        }

        [Test]
        public void PutPageBlob_WithMetadata_UploadsMetadata()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            var expectedMetadata = new Dictionary<string, string>(){
                { "firstValue", "1" },
                { "secondValue", "2"}
            };
            int expectedSize = 512;

            client.PutPageBlob(containerName, blobName, expectedSize, metadata: expectedMetadata);

            var blob = AssertBlobExists(containerName, blobName);
            Assert.IsTrue(blob.Metadata.Any(m => m.Key == "firstValue" && m.Value == "1"), "First value is missing or incorrect");
            Assert.IsTrue(blob.Metadata.Any(m => m.Key == "secondValue" && m.Value == "2"), "Second value is missing or incorrect");
        }

        [Test]
        public void PutPageBlob_WithSequenceNumber_AssignsSpecifiedSequenceNumberToBlob()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);
            var data = UTF8Encoding.UTF8.GetBytes("unit test content");
            int expectedSize = 512;
            long expectedSequenceNumber = 123;

            client.PutPageBlob(containerName, blobName, expectedSize, sequenceNumber: expectedSequenceNumber);

            var blob = AssertBlobExists(containerName, blobName);
            Assert.AreEqual(expectedSequenceNumber, blob.Properties.PageBlobSequenceNumber);
        }

        [Test]
        public async Task PutPageBlobAsync_WithRequiredArgs_CreatesNewPageBlob()
        {
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);
            int expectedSize = 512;

            await client.PutPageBlobAsync(containerName, blobName, expectedSize);

            var blob = AssertBlobExists(containerName, blobName);
            Assert.AreEqual(expectedSize, blob.Properties.Length);
        }


        #endregion

        #region Assertions

        private void AssertContainerExists(string containerName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail(String.Format("The container '{0}' does not exist", containerName));
        }

        private void AssertContainerAccess(string containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType containerAccessType)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail(String.Format("The container '{0}' does not exist", containerName));

            var permissions = container.GetPermissions();
            Assert.AreEqual(containerAccessType, permissions.PublicAccess, String.Format("Container access was expected to be {0}, but it is actually {1}", containerAccessType, permissions.PublicAccess));
        }

        private Microsoft.WindowsAzure.Storage.Blob.ICloudBlob AssertBlobExists(string containerName, string blobName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail(String.Format("The container '{0}' does not exist", containerName));

            var blob = container.GetBlobReferenceFromServer(blobName);
            if (!blob.Exists())
                Assert.Fail(String.Format("The blob '{0}' does not exist", blobName));

            return blob;
        }

        private IDictionary<string, string> GetContainerMetadata(string containerName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);

            container.FetchAttributes();
            return container.Metadata;
        }

        #endregion

        #region Setup Mechanics

        private void CreateContainer(string containerName, Dictionary<string,string> metadata = null)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);

            container.Create();

            if(metadata != null){
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
            _containersToCleanUp[containerName] = lease;
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


        private void CreateBlob(string containerName, string blobName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);

            byte[] data = UTF8Encoding.UTF8.GetBytes("Generic content");
            blob.UploadFromByteArray(data, 0, data.Length);
        }

        #endregion

        private DateTime GetTruncatedUtcNow()
        {
            return new DateTime(DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day, DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, DateTime.UtcNow.Second, DateTimeKind.Utc);
        }

    }
}
