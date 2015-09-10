using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.BlobService.BlobOperations;
using Basic.Azure.Storage.Extensions;
using Basic.Azure.Storage.Extensions.Contracts;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NUnit.Framework;

namespace Basic.Azure.Storage.Tests.Integration
{
    [TestFixture]
    public class BlobServiceClientExtensionsTests
    {
        #region setup
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
            var client = new BlobServiceClientEx(_accountSettings);

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

        #endregion

        #region PutBlockBlobIntelligently

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_DefaultArgsOnly_CreatesBlockBlob()
        {
            var expectedData = Encoding.UTF8.GetBytes("test data");
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            // Tempt it to do it in two uploads by specifying half megabyte
            await client.PutBlockBlobIntelligentlyAsync(expectedData.Length - 5, containerName, blobName, expectedData);

            AssertBlockBlobExists(containerName, blobName);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public void PutBlockBlobIntelligently_DefaultArgsOnly_CreatesBlockBlob()
        {
            var expectedData = Encoding.UTF8.GetBytes("test data");
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            // Tempt it to do it in two uploads by specifying half megabyte
            client.PutBlockBlobIntelligently(expectedData.Length - 5, containerName, blobName, expectedData);

            AssertBlockBlobExists(containerName, blobName);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_DataSmallerThanMaxSingleUploadSize_CreatesBlockBlobWithOneUpload()
        {
            var expectedData = Encoding.UTF8.GetBytes("test data");
            var expectedDataLength = expectedData.Length;
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            // Tempt it to do it in two uploads by specifying a blocksize smaller than the data length
            var response = await client.PutBlockBlobIntelligentlyAsync(expectedDataLength - 5, containerName, blobName, expectedData);

            AssertBlobOfSingleUpload(expectedData, containerName, blobName);
            AssertIsPutBlockBlobResponse(response);
        }

        [Test]
        public void PutBlockBlobIntelligently_DataSmallerThanMaxSingleUploadSize_CreatesBlockBlobWithOneUpload()
        {
            var expectedData = Encoding.UTF8.GetBytes("test data");
            var expectedDataLength = expectedData.Length;
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            // Tempt it to do it in two uploads by specifying a blocksize smaller than the data length
            var response = client.PutBlockBlobIntelligently(expectedDataLength - 5, containerName, blobName, expectedData);

            AssertBlobOfSingleUpload(expectedData, containerName, blobName);
            AssertIsPutBlockBlobResponse(response);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_DataLargerThanMaxSingleUploadSize_CreatesBlockBlobWithMultipleUploads()
        {
            var expectedData = new byte[BlobServiceConstants.MaxSingleBlobUploadSize + 5];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            var response = await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData);

            AssertBlobOfMultipleUploads(expectedData, containerName, blobName);
            AssertIsBlockListResponse(response);
        }

        [Test]
        public void PutBlockBlobIntelligently_DataLargerThanMaxSingleUploadSize_CreatesBlockBlobWithMultipleUploads()
        {
            var expectedData = new byte[BlobServiceConstants.MaxSingleBlobUploadSize + 5];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            var response = client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData);

            AssertBlobOfMultipleUploads(expectedData, containerName, blobName);
            AssertIsBlockListResponse(response);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_MultipleUploadsWithContentType_CorrectContentTypeSet()
        {
            const string expectedContentType = "foo/bar";
            var expectedData = new byte[BlobServiceConstants.MaxSingleBlobUploadSize + 5];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentType: expectedContentType);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfMultipleUploads(expectedData, containerName, blobName);
            Assert.AreEqual(expectedContentType, properties.ContentType);
        }

        [Test]
        public void PutBlockBlobIntelligently_MultipleUploadsWithContentType_CorrectContentTypeSet()
        {
            const string expectedContentType = "foo/bar";
            var expectedData = new byte[BlobServiceConstants.MaxSingleBlobUploadSize + 5];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentType: expectedContentType);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfMultipleUploads(expectedData, containerName, blobName);
            Assert.AreEqual(expectedContentType, properties.ContentType);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_SingleUploadWithContentType_CorrectContentTypeSet()
        {
            const string expectedContentType = "foo/bar";
            var expectedData = new byte[1024];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentType: expectedContentType);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfSingleUpload(expectedData, containerName, blobName);
            Assert.AreEqual(expectedContentType, properties.ContentType);
        }

        [Test]
        public void PutBlockBlobIntelligently_SingleUploadWithContentType_CorrectContentTypeSet()
        {
            const string expectedContentType = "foo/bar";
            var expectedData = new byte[1024];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentType: expectedContentType);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfSingleUpload(expectedData, containerName, blobName);
            Assert.AreEqual(expectedContentType, properties.ContentType);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_MultipleUploadsWithContentLanguage_CorrectContentLanguage()
        {
            const string expectedContentLanguage = "ancient/yiddish";
            var expectedData = new byte[BlobServiceConstants.MaxSingleBlobUploadSize + 5];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentLanguage: expectedContentLanguage);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfMultipleUploads(expectedData, containerName, blobName);
            Assert.AreEqual(expectedContentLanguage, properties.ContentLanguage);
        }

        [Test]
        public void PutBlockBlobIntelligently_MultipleUploadsWithContentLanguage_CorrectContentLanguage()
        {
            const string expectedContentLanguage = "ancient/yiddish";
            var expectedData = new byte[BlobServiceConstants.MaxSingleBlobUploadSize + 5];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentLanguage: expectedContentLanguage);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfMultipleUploads(expectedData, containerName, blobName);
            Assert.AreEqual(expectedContentLanguage, properties.ContentLanguage);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_SingleUploadWithContentLanguage_CorrectContentLanguage()
        {
            const string expectedContentLanguage = "ancient/yiddish";
            var expectedData = new byte[1024];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentLanguage: expectedContentLanguage);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfSingleUpload(expectedData, containerName, blobName);
            Assert.AreEqual(expectedContentLanguage, properties.ContentLanguage);
        }

        [Test]
        public void PutBlockBlobIntelligently_SingleUploadWithContentLanguage_CorrectContentLanguage()
        {
            const string expectedContentLanguage = "ancient/yiddish";
            var expectedData = new byte[1024];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentLanguage: expectedContentLanguage);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfSingleUpload(expectedData, containerName, blobName);
            Assert.AreEqual(expectedContentLanguage, properties.ContentLanguage);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_MultipleUploadsWithContentEncoding_CorrectContentEncoding()
        {
            const string expectedContentEncoding = "UTF8";
            var expectedData = new byte[BlobServiceConstants.MaxSingleBlobUploadSize + 5];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentEncoding: expectedContentEncoding);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfMultipleUploads(expectedData, containerName, blobName);
            Assert.AreEqual(expectedContentEncoding, properties.ContentEncoding);
        }

        [Test]
        public void PutBlockBlobIntelligently_MultipleUploadsWithContentEncoding_CorrectContentEncoding()
        {
            const string expectedContentEncoding = "UTF8";
            var expectedData = new byte[BlobServiceConstants.MaxSingleBlobUploadSize + 5];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentEncoding: expectedContentEncoding);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfMultipleUploads(expectedData, containerName, blobName);
            Assert.AreEqual(expectedContentEncoding, properties.ContentEncoding);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_SingleUploadWithContentEncoding_CorrectContentEncoding()
        {
            const string expectedContentEncoding = "UTF8";
            var expectedData = new byte[1024];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentEncoding: expectedContentEncoding);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfSingleUpload(expectedData, containerName, blobName);
            Assert.AreEqual(expectedContentEncoding, properties.ContentEncoding);
        }

        [Test]
        public void PutBlockBlobIntelligently_SingleUploadWithContentEncoding_CorrectContentEncoding()
        {
            const string expectedContentEncoding = "UTF8";
            var expectedData = new byte[1024];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentEncoding: expectedContentEncoding);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfSingleUpload(expectedData, containerName, blobName);
            Assert.AreEqual(expectedContentEncoding, properties.ContentEncoding);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_MultipleUploadsWithContentMD5_CorrectContentMD5()
        {
            var expectedData = new byte[BlobServiceConstants.MaxSingleBlobUploadSize + 5];
            var expectedContentMD5 = Convert.ToBase64String(MD5.Create().ComputeHash(expectedData));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentMD5: expectedContentMD5);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfMultipleUploads(expectedData, containerName, blobName);
            Assert.AreEqual(expectedContentMD5, properties.ContentMD5);
        }

        [Test]
        public void PutBlockBlobIntelligently_MultipleUploadsWithContentMD5_CorrectContentMD5()
        {
            var expectedData = new byte[BlobServiceConstants.MaxSingleBlobUploadSize + 5];
            var expectedContentMD5 = Convert.ToBase64String(MD5.Create().ComputeHash(expectedData));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentMD5: expectedContentMD5);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfMultipleUploads(expectedData, containerName, blobName);
            Assert.AreEqual(expectedContentMD5, properties.ContentMD5);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_SingleUploadWithContentMD5_CorrectContentMD5()
        {
            var expectedData = new byte[1024];
            var expectedContentMD5 = Convert.ToBase64String(MD5.Create().ComputeHash(expectedData));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentMD5: expectedContentMD5);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfSingleUpload(expectedData, containerName, blobName);
            Assert.AreEqual(expectedContentMD5, properties.ContentMD5);
        }

        [Test]
        public void PutBlockBlobIntelligently_SingleUploadWithContentMD5_CorrectContentMD5()
        {
            var expectedData = new byte[1024];
            var expectedContentMD5 = Convert.ToBase64String(MD5.Create().ComputeHash(expectedData));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentMD5: expectedContentMD5);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfSingleUpload(expectedData, containerName, blobName);
            Assert.AreEqual(expectedContentMD5, properties.ContentMD5);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_MultipleUploadsWithCacheControl_CorrectContentCacheControl()
        {
            var expectedData = new byte[BlobServiceConstants.MaxSingleBlobUploadSize + 5];
            const string expectedCacheControl = "ponyfoo";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                cacheControl: expectedCacheControl);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfMultipleUploads(expectedData, containerName, blobName);
            Assert.AreEqual(expectedCacheControl, properties.CacheControl);
        }

        [Test]
        public void PutBlockBlobIntelligently_MultipleUploadsWithCacheControl_CorrectContentCacheControl()
        {
            var expectedData = new byte[BlobServiceConstants.MaxSingleBlobUploadSize + 5];
            const string expectedCacheControl = "ponyfoo";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                cacheControl: expectedCacheControl);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfMultipleUploads(expectedData, containerName, blobName);
            Assert.AreEqual(expectedCacheControl, properties.CacheControl);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_SingleUploadWithCacheControl_CorrectContentCacheControl()
        {
            var expectedData = new byte[1024];
            const string expectedCacheControl = "ponyfoo";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                cacheControl: expectedCacheControl);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfSingleUpload(expectedData, containerName, blobName);
            Assert.AreEqual(expectedCacheControl, properties.CacheControl);
        }

        [Test]
        public void PutBlockBlobIntelligently_SingleUploadWithCacheControl_CorrectContentCacheControl()
        {
            var expectedData = new byte[1024];
            const string expectedCacheControl = "ponyfoo";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                cacheControl: expectedCacheControl);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlobOfSingleUpload(expectedData, containerName, blobName);
            Assert.AreEqual(expectedCacheControl, properties.CacheControl);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_MultipleUploadsWithMetadata_CorrectMetadata()
        {
            var expectedData = new byte[BlobServiceConstants.MaxSingleBlobUploadSize + 5];
            var expectedMetadata = new Dictionary<string, string>
            {
                {"haikuLine1", "I dreamed I was in"},
                {"haikuLine2", "A long-running endless loop"},
                {"haikuLine3", "And then the next day"}
            };
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                metadata: expectedMetadata);
            var metadata = GetBlobMetadata(containerName, blobName);

            AssertBlobOfMultipleUploads(expectedData, containerName, blobName);
            Assert.AreEqual(expectedMetadata, metadata);
        }

        [Test]
        public void PutBlockBlobIntelligently_MultipleUploadsWithMetadata_CorrectMetadata()
        {
            var expectedData = new byte[BlobServiceConstants.MaxSingleBlobUploadSize + 5];
            var expectedMetadata = new Dictionary<string, string>
            {
                {"haikuLine1", "I dreamed I was in"},
                {"haikuLine2", "A long-running endless loop"},
                {"haikuLine3", "And then the next day"}
            };
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                metadata: expectedMetadata);
            var metadata = GetBlobMetadata(containerName, blobName);

            AssertBlobOfMultipleUploads(expectedData, containerName, blobName);
            Assert.AreEqual(expectedMetadata, metadata);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_SingleUploadWithMetadata_CorrectMetadata()
        {
            var expectedData = new byte[1024];
            var expectedMetadata = new Dictionary<string, string>
            {
                {"haikuLine1", "I dreamed I was in"},
                {"haikuLine2", "A long-running endless loop"},
                {"haikuLine3", "And then the next day"}
            };
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                metadata: expectedMetadata);
            var metadata = GetBlobMetadata(containerName, blobName);

            AssertBlobOfSingleUpload(expectedData, containerName, blobName);
            Assert.AreEqual(expectedMetadata, metadata);
        }

        [Test]
        public void PutBlockBlobIntelligently_SingleUploadWithMetadata_CorrectMetadata()
        {
            var expectedData = new byte[1024];
            var expectedMetadata = new Dictionary<string, string>
            {
                {"haikuLine1", "I dreamed I was in"},
                {"haikuLine2", "A long-running endless loop"},
                {"haikuLine3", "And then the next day"}
            };
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
            CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                metadata: expectedMetadata);
            var metadata = GetBlobMetadata(containerName, blobName);

            AssertBlobOfSingleUpload(expectedData, containerName, blobName);
            Assert.AreEqual(expectedMetadata, metadata);
        }

        #endregion

        #region PutBlockBlobAsList

        [Test]
        public async void PutBlockBlobAsListAsync_RequiredArgsOnly_CreatesBlockBlobFromLatestBlocks()
        {
            const string dataPerBlock = "foo";
            var expectedData = Encoding.UTF8.GetBytes(string.Format("{0}{0}{0}", dataPerBlock));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);

            await client.PutBlockBlobAsListAsync(4 * megabyte, containerName, blobName, expectedData);

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
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);

            client.PutBlockBlobAsList(4 * 1024 * 1024, containerName, blobName, expectedData);

            AssertBlockBlobExists(containerName, blobName);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_BlockSizeLargerThanBlob_CreatesBlobWithoutError()
        {
            var expectedData = new byte[10];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);

            await client.PutBlockBlobAsListAsync(expectedData.Length * 2, containerName, blobName, expectedData);

            AssertBlockBlobExists(containerName, blobName);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public void PutBlockBlobAsList_BlockSizeLargerThanBlob_CreatesBlobWithoutError()
        {
            var expectedData = new byte[10];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);

            client.PutBlockBlobAsList(expectedData.Length * 2, containerName, blobName, expectedData);

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
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);

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
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);

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
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);

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
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);

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
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);

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
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);

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
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);

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
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);

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
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);

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
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);

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
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);

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
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);

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
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);

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
            IBlobServiceClientEx client = new BlobServiceClientEx(_accountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, contentMD5: specifiedMismatchedContentMD5);
            var properties = GetBlobProperties(containerName, blobName);

            AssertBlockBlobExists(containerName, blobName);
            Assert.AreEqual(specifiedMismatchedContentMD5, properties.ContentMD5);
        }

        #endregion

        #region Assertions

        private static void AssertIsPutBlockBlobResponse(IBlobOrBlockListResponseWrapper response)
        {
            Assert.IsTrue(response.IsPutBlobResponse);
            Assert.IsInstanceOf(typeof(PutBlobResponse), response.Response);
        }

        private static void AssertIsBlockListResponse(IBlobOrBlockListResponseWrapper response)
        {
            Assert.IsTrue(response.IsPutBlockListResponse);
            Assert.IsInstanceOf(typeof(PutBlockListResponse), response.Response);
        }

        private void AssertBlobOfSingleUpload(byte[] expectedData, string containerName, string blobName)
        {
            Assert.LessOrEqual(expectedData.Length, BlobServiceConstants.MaxSingleBlobUploadSize);
            AssertBlockBlobExists(containerName, blobName);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        private void AssertBlobOfMultipleUploads(byte[] expectedData, string containerName, string blobName)
        {
            Assert.Greater(expectedData.Length, BlobServiceConstants.MaxSingleBlobUploadSize);
            AssertBlockBlobExists(containerName, blobName);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        private void AssertBlockBlobExists(string containerName, string blobName)
        {
            var client = new BlobServiceClientEx(_accountSettings);
            var blobList = client.ListBlobs(containerName);

            Assert.IsTrue(blobList.BlobList.Any(blob => blob.Name == blobName));
        }

        private void AssertBlockBlobContainsData(string containerName, string blobName, byte[] expectedData)
        {
            var client = new BlobServiceClientEx(_accountSettings);
            var blob = client.GetBlob(containerName, blobName);

            var gottenData = blob.GetDataBytes();

            Assert.AreEqual(expectedData.Length, gottenData.Length);

            // Comparing strings -> MUCH faster than comparing the raw arrays
            // However, bugs out sometimes if the data is too large
            try
            {
                var gottenDataString = Encoding.Unicode.GetString(gottenData);
                var expectedDataString = Encoding.Unicode.GetString(expectedData);

                Assert.AreEqual(gottenDataString, expectedDataString);
            }
            catch
            {
                // Compare raw arrays as last resort...
                // This only happens if the test running process doesn't have enough memory to convert
                Assert.AreEqual(expectedData, gottenData);
            }
        }

        #endregion

        #region Setup Mechanics

        private void CreateContainer(string containerName, Dictionary<string, string> metadata = null)
        {
            var client = new BlobServiceClientEx(_accountSettings);

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
