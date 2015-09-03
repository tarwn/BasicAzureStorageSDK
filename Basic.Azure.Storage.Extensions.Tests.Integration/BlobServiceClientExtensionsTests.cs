using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.BlobService;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NUnit.Framework;

namespace Basic.Azure.Storage.Extensions.Tests.Integration
{
    [TestFixture]
    public class BlobServiceClientExtensionsTests
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

        private static string GenerateSampleBlobName()
        {
            return string.Format("unit-test-{0}", Guid.NewGuid());
        }

        private void RegisterContainerForCleanup(string containerName, string leaseId)
        {
            _containersToCleanUp[containerName] = leaseId;
        }

        [TestFixtureTearDown]
        public void TestFixtureTeardown()
        {
            //let's clean up!
            var client = new BlobServiceClient(_accountSettings);

            //var client = _storageAccount.CreateCloudBlobClient();
            foreach (var containerPair in _containersToCleanUp)
            {
                try
                {
                    client.DeleteContainer(containerPair.Key, containerPair.Value);
                }
                catch
                {
                    // ignored
                }
            }
        }

        [Test]
        public async void PutBlockBlobAsListAsync_RequiredArgsOnly_CreatesBlockBlobFromLatestBlocks()
        {
            const string dataPerBlock = "foo";
            var expectedData = Encoding.UTF8.GetBytes(string.Format("{0}{0}{0}", dataPerBlock));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            CreateContainer(containerName);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, expectedData);

            AssertBlockBlobExists(containerName, blobName);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public void PutBlockBlobAsList_RequiredArgsOnly_CreatesBlockBlobFromLatestBlocks()
        {
            const string dataPerBlock = "foo";
            var expectedData = Encoding.UTF8.GetBytes(string.Format("{0}{0}{0}", dataPerBlock));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            CreateContainer(containerName);

            client.PutBlockBlobAsList(4, containerName, blobName, expectedData);

            AssertBlockBlobExists(containerName, blobName);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_OneByteAtATime_CreatesBlobOfCorrectLength()
        {
            const string dataPerBlock = "foo";
            var expectedData = Encoding.UTF8.GetBytes(string.Format("{0}{0}{0}", dataPerBlock));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            CreateContainer(containerName);

            await client.PutBlockBlobAsListAsync(1, containerName, blobName, expectedData);

            AssertBlockBlobExists(containerName, blobName);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public void PutBlockBlobAsList_OneByteAtATime_CreatesBlobOfCorrectLength()
        {
            const string dataPerBlock = "foo";
            var expectedData = Encoding.UTF8.GetBytes(string.Format("{0}{0}{0}", dataPerBlock));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);
            CreateContainer(containerName);

            client.PutBlockBlobAsList(1, containerName, blobName, expectedData);

            AssertBlockBlobExists(containerName, blobName);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_LargerThan64MB_CreatesBlockBlobFromLatestBlocks()
        {
            const int megabyte = 1024 * 1024;
            var expectedData = new byte[65 * megabyte];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            await client.PutBlockBlobAsListAsync(4 * 1024 * 1024, containerName, blobName, expectedData);

            AssertBlockBlobExists(containerName, blobName);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public void PutBlockBlobAsList_LargerThan64MB_CreatesBlockBlobFromLatestBlocks()
        {
            const int megabyte = 1024 * 1024;
            var expectedData = new byte[65 * megabyte];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            client.PutBlockBlobAsList(4 * 1024 * 1024, containerName, blobName, expectedData);

            AssertBlockBlobExists(containerName, blobName);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_SpecifiedContentType_CreatesBlockBlobWithSpecifiedContentType()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            const string specifiedContentType = "test/content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, contentType: specifiedContentType);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlockBlobExists(containerName, blobName);
            Assert.AreEqual(specifiedContentType, properties.ContentType);
        }

        [Test]
        public void PutBlockBlobAsList_SpecifiedContentType_CreatesBlockBlobWithSpecifiedContentType()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            const string specifiedContentType = "test/content";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, contentType: specifiedContentType);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlockBlobExists(containerName, blobName);
            Assert.AreEqual(specifiedContentType, properties.ContentType);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_SpecifiedContentEncoding_CreatesBlockBlobWithSpecifiedContentEncoding()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            const string specifiedContentEncoding = "UTF8";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, contentEncoding: specifiedContentEncoding);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlockBlobExists(containerName, blobName);
            Assert.AreEqual(specifiedContentEncoding, properties.ContentEncoding);
        }

        [Test]
        public void PutBlockBlobAsList_SpecifiedContentEncoding_CreatesBlockBlobWithSpecifiedContentEncoding()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            const string specifiedContentEncoding = "UTF8";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, contentEncoding: specifiedContentEncoding);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlockBlobExists(containerName, blobName);
            Assert.AreEqual(specifiedContentEncoding, properties.ContentEncoding);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_SpecifiedContentLanguage_CreatesBlockBlobWithSpecifiedContentLanguage()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            const string specifiedContentLanguage = "typeB";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, contentLanguage: specifiedContentLanguage);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlockBlobExists(containerName, blobName);
            Assert.AreEqual(specifiedContentLanguage, properties.ContentLanguage);
        }

        [Test]
        public void PutBlockBlobAsList_SpecifiedContentLanguage_CreatesBlockBlobWithSpecifiedContentLanguage()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            const string specifiedContentLanguage = "typeB";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, contentLanguage: specifiedContentLanguage);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlockBlobExists(containerName, blobName);
            Assert.AreEqual(specifiedContentLanguage, properties.ContentLanguage);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_SpecifiedCacheControl_CreatesBlockBlobWithSpecifiedCacheControl()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            const string specifiedCacheControl = "foo";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, cacheControl: specifiedCacheControl);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlockBlobExists(containerName, blobName);
            Assert.AreEqual(specifiedCacheControl, properties.CacheControl);
        }

        [Test]
        public void PutBlockBlobAsList_SpecifiedCacheControl_CreatesBlockBlobWithSpecifiedCacheControl()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            const string specifiedCacheControl = "foo";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, cacheControl: specifiedCacheControl);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlockBlobExists(containerName, blobName);
            Assert.AreEqual(specifiedCacheControl, properties.CacheControl);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_SpecifiedMetadata_CreatesBlockBlobWithSpecifiedMetadata()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            var expectedMetadata = new Dictionary<string, string>
            {
                {"haikuLine1", "I dreamed I was in"},
                {"haikuLine2", "A long-running endless loop"},
                {"haikuLine3", "And then the next day"}
            };
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, metadata: expectedMetadata);
            var gottenMetadata = GetBlobMetadata(containerName, blobName);

            AssertBlockBlobExists(containerName, blobName);
            Assert.AreEqual(expectedMetadata, gottenMetadata);
        }

        [Test]
        public void PutBlockBlobAsList_SpecifiedMetadata_CreatesBlockBlobWithSpecifiedMetadata()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            var expectedMetadata = new Dictionary<string, string>
            {
                {"haikuLine1", "I dreamed I was in"},
                {"haikuLine2", "A long-running endless loop"},
                {"haikuLine3", "And then the next day"}
            };
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, metadata: expectedMetadata);
            var gottenMetadata = GetBlobMetadata(containerName, blobName);

            AssertBlockBlobExists(containerName, blobName);
            Assert.AreEqual(expectedMetadata, gottenMetadata);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_SpecifiedContentMD5_CreatesBlockBlobWithSpecifiedContentMD5()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            var specifiedContentMD5 = Convert.ToBase64String(MD5.Create().ComputeHash(data));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, contentMD5: specifiedContentMD5);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlockBlobExists(containerName, blobName);
            Assert.AreEqual(specifiedContentMD5, properties.ContentMD5);
        }

        [Test]
        public void PutBlockBlobAsList_SpecifiedContentMD5_CreatesBlockBlobWithSpecifiedContentMD5()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            var specifiedContentMD5 = Convert.ToBase64String(MD5.Create().ComputeHash(data));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, contentMD5: specifiedContentMD5);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlockBlobExists(containerName, blobName);
            Assert.AreEqual(specifiedContentMD5, properties.ContentMD5);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_MismatchedContentMD5_CreatesBlockBlobWithSpecifiedContentMD5()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            var badData = Encoding.UTF8.GetBytes("bad content");
            var specifiedMismatchedContentMD5 = Convert.ToBase64String(MD5.Create().ComputeHash(badData));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, contentMD5: specifiedMismatchedContentMD5);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlockBlobExists(containerName, blobName);
            Assert.AreEqual(specifiedMismatchedContentMD5, properties.ContentMD5);
        }

        [Test]
        public void PutBlockBlobAsList_MismatchedContentMD5_CreatesBlockBlobWithSpecifiedContentMD5()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            var badData = Encoding.UTF8.GetBytes("bad content");
            var specifiedMismatchedContentMD5 = Convert.ToBase64String(MD5.Create().ComputeHash(badData));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(_accountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, contentMD5: specifiedMismatchedContentMD5);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlockBlobExists(containerName, blobName);
            Assert.AreEqual(specifiedMismatchedContentMD5, properties.ContentMD5);
        }

        #region Assertions

        private void AssertBlockBlobExists(string containerName, string blobName)
        {
            var client = new BlobServiceClient(_accountSettings);
            var blobList = client.ListBlobs(containerName);

            Assert.IsTrue(blobList.BlobList.Any(blob => blob.Name == blobName));
        }

        private void AssertBlockBlobContainsData(string containerName, string blobName, byte[] expectedData)
        {
            var client = new BlobServiceClient(_accountSettings);
            var blob = client.GetBlob(containerName, blobName);

            var gottenData = blob.GetDataBytes();

            // Comparing strings -> MUCH faster than comparing the raw arrays
            var gottenDataString = Convert.ToBase64String(gottenData);
            var expectedDataString = Convert.ToBase64String(expectedData);

            Assert.AreEqual(expectedData.Length, gottenData.Length);
            Assert.AreEqual(gottenDataString, expectedDataString);

        }

        #endregion

        #region Setup Mechanics

        private void CreateContainer(string containerName, Dictionary<string, string> metadata = null)
        {
            var client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.None);

            if (metadata != null)
            {
                client.SetContainerMetadata(containerName, metadata);
            }
        }

        private BlobProperties GetBlobProperties(string containerName, string blobName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail("GetBlobProperties: The container '{0}' does not exist", containerName);

            var blob = container.GetBlockBlobReference(blobName);

            if (!blob.Exists())
                Assert.Fail("GetBlobProperties: The blob '{0}' does not exist", blobName);

            return blob.Properties;
        }

        private IDictionary<string, string> GetBlobMetadata(string containerName, string blobName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail("GetBlobProperties: The container '{0}' does not exist", containerName);

            var blob = container.GetBlockBlobReference(blobName);

            if (!blob.Exists())
                Assert.Fail("GetBlobProperties: The blob '{0}' does not exist", blobName);

            return blob.Metadata;
        }

        #endregion

    }
}
