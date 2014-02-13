using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
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

        private List<string> _containersToCleanUp = new List<string>();

        private string GenerateSampleContainerName()
        {
            var name = "unit-test-" + Guid.NewGuid().ToString().ToLower();
            _containersToCleanUp.Add(name);
            return name;
        }

        private string GenerateSampleBlobName()
        {
            return String.Format("unit-test-{0}", Guid.NewGuid());
        }

        [TestFixtureTearDown]
        public void TestFixtureTeardown()
        {
            //let's clean up!
            var client = _storageAccount.CreateCloudBlobClient();
            foreach (var containerName in _containersToCleanUp)
            {
                var container = client.GetContainerReference(containerName);
                container.DeleteIfExists();
            }
        }

        #region Container Operations Tests

        [Test]
        public void CreateContainer_ValidArguments_CreatesContainerWithSpecificName()
        {
            var containerName = GenerateSampleContainerName();
            var client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.None);

            AssertContainerExists(containerName);
        }

        [Test]
        public void CreateContainer_ValidArgumentsWithPublicContainerAccess_CreatesContainerWithContainerAccess()
        {
            var containerName = GenerateSampleContainerName();
            var client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.PublicContainer);

            AssertContainerAccess(containerName, BlobContainerPublicAccessType.Container);
        }

        [Test]
        public void CreateContainer_ValidArgumentsWithPublicBlobAccess_CreatesContainerWithBlobAccess()
        {
            var containerName = GenerateSampleContainerName();
            var client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.PublicBlob);

            AssertContainerAccess(containerName, BlobContainerPublicAccessType.Blob);
        }

        [Test]
        public void CreateContainer_ValidArgumentsWithNoPublicAccess_CreatesContainerWithNoPublicAccess()
        {
            var containerName = GenerateSampleContainerName();
            var client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.None);

            AssertContainerAccess(containerName, BlobContainerPublicAccessType.Off);
        }

        [Test]
        [ExpectedException(typeof(ContainerAlreadyExistsAzureException))]
        public void CreateContainer_AlreadyExists_ThrowsContainerAlreadyExistsException()
        {
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.None);

            // expects exception
        }

        [Test]
        public void CreateContainer_AlreadyExists_ReturnsContainerCreationResponse()
        {
            var containerName = GenerateSampleContainerName();
            var client = new BlobServiceClient(_accountSettings);

            var response = client.CreateContainer(containerName, ContainerAccessType.None);

            Assert.IsTrue(response.Date > DateTime.Now.AddSeconds(-5), String.Format("Response Date was set to {0}", response.Date));
            Assert.IsTrue(response.LastModified > DateTime.Now.AddSeconds(-5), String.Format("Response LastModified was set to {0}", response.LastModified));
            Assert.IsFalse(string.IsNullOrWhiteSpace(response.ETag), "Response ETag is not set");

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

        #endregion

        #region Assertions

        private void AssertContainerExists(string containerName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail(String.Format("The container '{0}' does not exist", containerName));
        }

        private void AssertContainerAccess(string containerName, BlobContainerPublicAccessType containerAccessType)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail(String.Format("The container '{0}' does not exist", containerName));

            var permissions = container.GetPermissions();
            Assert.AreEqual(containerAccessType, permissions.PublicAccess, String.Format("Container access was expected to be {0}, but it is actually {1}", containerAccessType, permissions.PublicAccess));
        }

        private ICloudBlob AssertBlobExists(string containerName, string blobName)
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

        #endregion

        #region Setup Mechanics

        private void CreateContainer(string containerName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            container.Create();
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

    }
}
