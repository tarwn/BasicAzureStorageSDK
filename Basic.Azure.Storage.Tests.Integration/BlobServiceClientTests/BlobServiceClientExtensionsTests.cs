using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.BlobService.BlobOperations;
using Basic.Azure.Storage.Extensions;
using Basic.Azure.Storage.Extensions.Contracts;
using NUnit.Framework;
using BlobType = Microsoft.WindowsAzure.Storage.Blob.BlobType;
using Basic.Azure.Storage.Tests.Integration.BlobServiceClientTests;
using System.Configuration;

namespace Basic.Azure.Storage.Tests.Integration
{
    [TestFixture]
    public class BlobServiceClientExtensionsTests
    {
        // Dramatically shrink the breaking point of when the intelligent blob upload will split into multiple uploads
        private const int MaxIntelligentSingleBlobUploadSizeOverride = BlobServiceConstants.MaxSingleBlockUploadSize + 5;
        private BlobUtil _util = new BlobUtil(ConfigurationManager.AppSettings["AzureConnectionString"]);
        private readonly string _runId = DateTime.Now.ToString("yyyy-MM-dd");

        protected StorageAccountSettings AccountSettings
        {
            get
            {
                return StorageAccountSettings.Parse(ConfigurationManager.AppSettings["AzureConnectionString"]);
            }
        }

        #region PutBlockBlobIntelligently

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_DefaultArgsOnly_CreatesBlockBlob()
        {
            var expectedData = Encoding.UTF8.GetBytes("test data");
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            // Tempt it to do it in two uploads by specifying half megabyte
            await client.PutBlockBlobIntelligentlyAsync(expectedData.Length - 5, containerName, blobName, expectedData);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            _util.AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public void PutBlockBlobIntelligently_DefaultArgsOnly_CreatesBlockBlob()
        {
            var expectedData = Encoding.UTF8.GetBytes("test data");
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            // Tempt it to do it in two uploads by specifying half megabyte
            client.PutBlockBlobIntelligently(expectedData.Length - 5, containerName, blobName, expectedData);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            _util.AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_DataSmallerThanMaxSingleUploadSize_CreatesBlockBlobWithOneUpload()
        {
            var expectedData = Encoding.UTF8.GetBytes("test data");
            var expectedDataLength = expectedData.Length;
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            // Tempt it to do it in two uploads by specifying a blocksize smaller than the data length
            var response = await client.PutBlockBlobIntelligentlyAsync(expectedDataLength - 5, containerName, blobName, expectedData);

            _util.AssertBlobOfSingleUpload(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            _util.AssertIsPutBlockBlobResponse(response);
        }

        [Test]
        public void PutBlockBlobIntelligently_DataSmallerThanMaxSingleUploadSize_CreatesBlockBlobWithOneUpload()
        {
            var expectedData = Encoding.UTF8.GetBytes("test data");
            var expectedDataLength = expectedData.Length;
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            // Tempt it to do it in two uploads by specifying a blocksize smaller than the data length
            var response = client.PutBlockBlobIntelligently(expectedDataLength - 5, containerName, blobName, expectedData);

            _util.AssertBlobOfSingleUpload(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            _util.AssertIsPutBlockBlobResponse(response);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_DataLargerThanMaxSingleUploadSize_CreatesBlockBlobWithMultipleUploads()
        {
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
            _util.CreateContainer(containerName);

            var response = await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData);

            _util.AssertBlobOfMultipleUploads(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            _util.AssertIsBlockListResponse(response);
        }

        [Test]
        public void PutBlockBlobIntelligently_DataLargerThanMaxSingleUploadSize_CreatesBlockBlobWithMultipleUploads()
        {
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
            _util.CreateContainer(containerName);

            var response = client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData);

            _util.AssertBlobOfMultipleUploads(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            _util.AssertIsBlockListResponse(response);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_MultipleUploadsWithContentType_CorrectContentTypeSet()
        {
            const string expectedContentType = "foo/bar";
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
            _util.CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentType: expectedContentType);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfMultipleUploads(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedContentType, properties.ContentType);
        }

        [Test]
        public void PutBlockBlobIntelligently_MultipleUploadsWithContentType_CorrectContentTypeSet()
        {
            const string expectedContentType = "foo/bar";
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
            _util.CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentType: expectedContentType);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfMultipleUploads(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedContentType, properties.ContentType);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_SingleUploadWithContentType_CorrectContentTypeSet()
        {
            const string expectedContentType = "foo/bar";
            var expectedData = new byte[1024];
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentType: expectedContentType);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfSingleUpload(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedContentType, properties.ContentType);
        }

        [Test]
        public void PutBlockBlobIntelligently_SingleUploadWithContentType_CorrectContentTypeSet()
        {
            const string expectedContentType = "foo/bar";
            var expectedData = new byte[1024];
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentType: expectedContentType);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfSingleUpload(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedContentType, properties.ContentType);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_MultipleUploadsWithContentLanguage_CorrectContentLanguage()
        {
            const string expectedContentLanguage = "ancient/yiddish";
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
            _util.CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentLanguage: expectedContentLanguage);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfMultipleUploads(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedContentLanguage, properties.ContentLanguage);
        }

        [Test]
        public void PutBlockBlobIntelligently_MultipleUploadsWithContentLanguage_CorrectContentLanguage()
        {
            const string expectedContentLanguage = "ancient/yiddish";
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
            _util.CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentLanguage: expectedContentLanguage);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfMultipleUploads(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedContentLanguage, properties.ContentLanguage);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_SingleUploadWithContentLanguage_CorrectContentLanguage()
        {
            const string expectedContentLanguage = "ancient/yiddish";
            var expectedData = new byte[1024];
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentLanguage: expectedContentLanguage);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfSingleUpload(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedContentLanguage, properties.ContentLanguage);
        }

        [Test]
        public void PutBlockBlobIntelligently_SingleUploadWithContentLanguage_CorrectContentLanguage()
        {
            const string expectedContentLanguage = "ancient/yiddish";
            var expectedData = new byte[1024];
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentLanguage: expectedContentLanguage);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfSingleUpload(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedContentLanguage, properties.ContentLanguage);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_MultipleUploadsWithContentEncoding_CorrectContentEncoding()
        {
            const string expectedContentEncoding = "UTF8";
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
            _util.CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentEncoding: expectedContentEncoding);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfMultipleUploads(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedContentEncoding, properties.ContentEncoding);
        }

        [Test]
        public void PutBlockBlobIntelligently_MultipleUploadsWithContentEncoding_CorrectContentEncoding()
        {
            const string expectedContentEncoding = "UTF8";
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
            _util.CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentEncoding: expectedContentEncoding);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfMultipleUploads(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedContentEncoding, properties.ContentEncoding);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_SingleUploadWithContentEncoding_CorrectContentEncoding()
        {
            const string expectedContentEncoding = "UTF8";
            var expectedData = new byte[1024];
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentEncoding: expectedContentEncoding);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfSingleUpload(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedContentEncoding, properties.ContentEncoding);
        }

        [Test]
        public void PutBlockBlobIntelligently_SingleUploadWithContentEncoding_CorrectContentEncoding()
        {
            const string expectedContentEncoding = "UTF8";
            var expectedData = new byte[1024];
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentEncoding: expectedContentEncoding);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfSingleUpload(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedContentEncoding, properties.ContentEncoding);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_MultipleUploadsWithContentMD5_CorrectContentMD5()
        {
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var expectedContentMD5 = Convert.ToBase64String(MD5.Create().ComputeHash(expectedData));
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
            _util.CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentMD5: expectedContentMD5);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfMultipleUploads(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedContentMD5, properties.ContentMD5);
        }

        [Test]
        public void PutBlockBlobIntelligently_MultipleUploadsWithContentMD5_CorrectContentMD5()
        {
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var expectedContentMD5 = Convert.ToBase64String(MD5.Create().ComputeHash(expectedData));
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
            _util.CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentMD5: expectedContentMD5);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfMultipleUploads(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedContentMD5, properties.ContentMD5);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_SingleUploadWithContentMD5_CorrectContentMD5()
        {
            var expectedData = new byte[1024];
            var expectedContentMD5 = Convert.ToBase64String(MD5.Create().ComputeHash(expectedData));
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentMD5: expectedContentMD5);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfSingleUpload(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedContentMD5, properties.ContentMD5);
        }

        [Test]
        public void PutBlockBlobIntelligently_SingleUploadWithContentMD5_CorrectContentMD5()
        {
            var expectedData = new byte[1024];
            var expectedContentMD5 = Convert.ToBase64String(MD5.Create().ComputeHash(expectedData));
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                contentMD5: expectedContentMD5);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfSingleUpload(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedContentMD5, properties.ContentMD5);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_MultipleUploadsWithCacheControl_CorrectContentCacheControl()
        {
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            const string expectedCacheControl = "ponyfoo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
            _util.CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                cacheControl: expectedCacheControl);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfMultipleUploads(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedCacheControl, properties.CacheControl);
        }

        [Test]
        public void PutBlockBlobIntelligently_MultipleUploadsWithCacheControl_CorrectContentCacheControl()
        {
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            const string expectedCacheControl = "ponyfoo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
            _util.CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                cacheControl: expectedCacheControl);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfMultipleUploads(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedCacheControl, properties.CacheControl);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_SingleUploadWithCacheControl_CorrectContentCacheControl()
        {
            var expectedData = new byte[1024];
            const string expectedCacheControl = "ponyfoo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                cacheControl: expectedCacheControl);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfSingleUpload(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedCacheControl, properties.CacheControl);
        }

        [Test]
        public void PutBlockBlobIntelligently_SingleUploadWithCacheControl_CorrectContentCacheControl()
        {
            var expectedData = new byte[1024];
            const string expectedCacheControl = "ponyfoo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                cacheControl: expectedCacheControl);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobOfSingleUpload(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedCacheControl, properties.CacheControl);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_MultipleUploadsWithMetadata_CorrectMetadata()
        {
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var expectedMetadata = new Dictionary<string, string>
            {
                {"haikuLine1", "I dreamed I was in"},
                {"haikuLine2", "A long-running endless loop"},
                {"haikuLine3", "And then the next day"}
            };
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
            _util.CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                metadata: expectedMetadata);

            var metadata = _util.GetBlobMetadata(containerName, blobName);
            _util.AssertBlobOfMultipleUploads(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedMetadata, metadata);
        }

        [Test]
        public void PutBlockBlobIntelligently_MultipleUploadsWithMetadata_CorrectMetadata()
        {
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var expectedMetadata = new Dictionary<string, string>
            {
                {"haikuLine1", "I dreamed I was in"},
                {"haikuLine2", "A long-running endless loop"},
                {"haikuLine3", "And then the next day"}
            };
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
            _util.CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                metadata: expectedMetadata);

            var metadata = _util.GetBlobMetadata(containerName, blobName);
            _util.AssertBlobOfMultipleUploads(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
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
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                metadata: expectedMetadata);

            var metadata = _util.GetBlobMetadata(containerName, blobName);
            _util.AssertBlobOfSingleUpload(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
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
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                metadata: expectedMetadata);

            var metadata = _util.GetBlobMetadata(containerName, blobName);
            _util.AssertBlobOfSingleUpload(expectedData, containerName, blobName, MaxIntelligentSingleBlobUploadSizeOverride);
            Assert.AreEqual(expectedMetadata, metadata);
        }

        #endregion

        #region PutBlockBlobAsList

        // TODO Test auto-re-leasing when stream upload is available and we can control how fast they upload

        [Test]
        public async void PutBlockBlobAsListAsync_RequiredArgsOnly_CreatesBlockBlobFromLatestBlocks()
        {
            const string dataPerBlock = "foo";
            var expectedData = Encoding.UTF8.GetBytes(string.Format("{0}{0}{0}", dataPerBlock));
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, expectedData);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            _util.AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public void PutBlockBlobAsList_RequiredArgsOnly_CreatesBlockBlobFromLatestBlocks()
        {
            const string dataPerBlock = "foo";
            var expectedData = Encoding.UTF8.GetBytes(string.Format("{0}{0}{0}", dataPerBlock));
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            client.PutBlockBlobAsList(4, containerName, blobName, expectedData);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            _util.AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_OneByteAtATime_CreatesBlobOfCorrectLength()
        {
            const string dataPerBlock = "foo";
            var expectedData = Encoding.UTF8.GetBytes(string.Format("{0}{0}{0}", dataPerBlock));
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            await client.PutBlockBlobAsListAsync(1, containerName, blobName, expectedData);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            _util.AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public void PutBlockBlobAsList_OneByteAtATime_CreatesBlobOfCorrectLength()
        {
            const string dataPerBlock = "foo";
            var expectedData = Encoding.UTF8.GetBytes(string.Format("{0}{0}{0}", dataPerBlock));
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            _util.CreateContainer(containerName);

            client.PutBlockBlobAsList(1, containerName, blobName, expectedData);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            _util.AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_LargerThanMaxSingleBlobSize_CreatesBlockBlobFromLatestBlocks()
        {
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            await client.PutBlockBlobAsListAsync(MaxIntelligentSingleBlobUploadSizeOverride / 2, containerName, blobName, expectedData);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            _util.AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public void PutBlockBlobAsList_LargerThanMaxSingleBlobSize_CreatesBlockBlobFromLatestBlocks()
        {
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            client.PutBlockBlobAsList(MaxIntelligentSingleBlobUploadSizeOverride / 2, containerName, blobName, expectedData);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            _util.AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_BlockSizeLargerThanBlob_CreatesBlobWithoutError()
        {
            var expectedData = new byte[10];
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            await client.PutBlockBlobAsListAsync(expectedData.Length * 2, containerName, blobName, expectedData);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            _util.AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public void PutBlockBlobAsList_BlockSizeLargerThanBlob_CreatesBlobWithoutError()
        {
            var expectedData = new byte[10];
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            client.PutBlockBlobAsList(expectedData.Length * 2, containerName, blobName, expectedData);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            _util.AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_SpecifiedContentType_CreatesBlockBlobWithSpecifiedContentType()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            const string specifiedContentType = "test/content";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, contentType: specifiedContentType);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(specifiedContentType, properties.ContentType);
        }

        [Test]
        public void PutBlockBlobAsList_SpecifiedContentType_CreatesBlockBlobWithSpecifiedContentType()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            const string specifiedContentType = "test/content";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, contentType: specifiedContentType);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(specifiedContentType, properties.ContentType);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_SpecifiedContentEncoding_CreatesBlockBlobWithSpecifiedContentEncoding()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            const string specifiedContentEncoding = "UTF8";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, contentEncoding: specifiedContentEncoding);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(specifiedContentEncoding, properties.ContentEncoding);
        }

        [Test]
        public void PutBlockBlobAsList_SpecifiedContentEncoding_CreatesBlockBlobWithSpecifiedContentEncoding()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            const string specifiedContentEncoding = "UTF8";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, contentEncoding: specifiedContentEncoding);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(specifiedContentEncoding, properties.ContentEncoding);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_SpecifiedContentLanguage_CreatesBlockBlobWithSpecifiedContentLanguage()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            const string specifiedContentLanguage = "typeB";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, contentLanguage: specifiedContentLanguage);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(specifiedContentLanguage, properties.ContentLanguage);
        }

        [Test]
        public void PutBlockBlobAsList_SpecifiedContentLanguage_CreatesBlockBlobWithSpecifiedContentLanguage()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            const string specifiedContentLanguage = "typeB";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, contentLanguage: specifiedContentLanguage);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(specifiedContentLanguage, properties.ContentLanguage);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_SpecifiedCacheControl_CreatesBlockBlobWithSpecifiedCacheControl()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            const string specifiedCacheControl = "foo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, cacheControl: specifiedCacheControl);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(specifiedCacheControl, properties.CacheControl);
        }

        [Test]
        public void PutBlockBlobAsList_SpecifiedCacheControl_CreatesBlockBlobWithSpecifiedCacheControl()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            const string specifiedCacheControl = "foo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, cacheControl: specifiedCacheControl);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
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
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, metadata: expectedMetadata);

            var gottenMetadata = _util.GetBlobMetadata(containerName, blobName);
            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
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
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, metadata: expectedMetadata);

            var gottenMetadata = _util.GetBlobMetadata(containerName, blobName);
            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedMetadata, gottenMetadata);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_SpecifiedContentMD5_CreatesBlockBlobWithSpecifiedContentMD5()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            var specifiedContentMD5 = Convert.ToBase64String(MD5.Create().ComputeHash(data));
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, contentMD5: specifiedContentMD5);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(specifiedContentMD5, properties.ContentMD5);
        }

        [Test]
        public void PutBlockBlobAsList_SpecifiedContentMD5_CreatesBlockBlobWithSpecifiedContentMD5()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            var specifiedContentMD5 = Convert.ToBase64String(MD5.Create().ComputeHash(data));
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, contentMD5: specifiedContentMD5);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(specifiedContentMD5, properties.ContentMD5);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_MismatchedContentMD5_CreatesBlockBlobWithSpecifiedContentMD5()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            var badData = Encoding.UTF8.GetBytes("bad content");
            var specifiedMismatchedContentMD5 = Convert.ToBase64String(MD5.Create().ComputeHash(badData));
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, contentMD5: specifiedMismatchedContentMD5);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(specifiedMismatchedContentMD5, properties.ContentMD5);
        }

        [Test]
        public void PutBlockBlobAsList_MismatchedContentMD5_CreatesBlockBlobWithSpecifiedContentMD5()
        {
            var data = Encoding.UTF8.GetBytes("test content");
            var badData = Encoding.UTF8.GetBytes("bad content");
            var specifiedMismatchedContentMD5 = Convert.ToBase64String(MD5.Create().ComputeHash(badData));
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, contentMD5: specifiedMismatchedContentMD5);

            var properties = _util.GetBlobProperties(containerName, blobName);
            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(specifiedMismatchedContentMD5, properties.ContentMD5);
        }

        #endregion

        #region Assertions

        #endregion

    }
}
