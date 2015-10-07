using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.BlobService.BlobOperations;
using Basic.Azure.Storage.Extensions;
using Basic.Azure.Storage.Extensions.Contracts;
using NUnit.Framework;
using BlobType = Microsoft.WindowsAzure.Storage.Blob.BlobType;

namespace Basic.Azure.Storage.Tests.Integration
{
    [TestFixture]
    public class BlobServiceClientExtensionsTests : BaseBlobServiceClientTestFixture
    {
        // Dramatically shrink the breaking point of when the intelligent blob upload will split into multiple uploads
        private const int MaxIntelligentSingleBlobUploadSizeOverride = BlobServiceConstants.MaxSingleBlockUploadSize + 5;

        #region PutBlockBlobIntelligently

        #region Stream

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_StreamWithDefaultArgsOnly_CreatesBlockBlob()
        {
            var expectedData = Encoding.UTF8.GetBytes("test data");
            using (var stream = new MemoryStream(expectedData))
            {
                var containerName = GenerateSampleContainerName();
                var blobName = GenerateSampleBlobName();
                IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
                CreateContainer(containerName);

                // Tempt it to do it in two uploads by specifying half megabyte
                await client.PutBlockBlobIntelligentlyAsync(expectedData.Length - 5, containerName, blobName, stream);

                AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
                AssertBlockBlobContainsData(containerName, blobName, expectedData);
            }
        }

        #endregion

        #region Byte Array

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_DefaultArgsOnly_CreatesBlockBlob()
        {
            var expectedData = Encoding.UTF8.GetBytes("test data");
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            CreateContainer(containerName);

            // Tempt it to do it in two uploads by specifying half megabyte
            await client.PutBlockBlobIntelligentlyAsync(expectedData.Length - 5, containerName, blobName, expectedData);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public void PutBlockBlobIntelligently_DefaultArgsOnly_CreatesBlockBlob()
        {
            var expectedData = Encoding.UTF8.GetBytes("test data");
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            CreateContainer(containerName);

            // Tempt it to do it in two uploads by specifying half megabyte
            client.PutBlockBlobIntelligently(expectedData.Length - 5, containerName, blobName, expectedData);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_DataSmallerThanMaxSingleUploadSize_CreatesBlockBlobWithOneUpload()
        {
            var expectedData = Encoding.UTF8.GetBytes("test data");
            var expectedDataLength = expectedData.Length;
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            CreateContainer(containerName);

            // Tempt it to do it in two uploads by specifying a blocksize smaller than the data length
            var response = client.PutBlockBlobIntelligently(expectedDataLength - 5, containerName, blobName, expectedData);

            AssertBlobOfSingleUpload(expectedData, containerName, blobName);
            AssertIsPutBlockBlobResponse(response);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_DataLargerThanMaxSingleUploadSize_CreatesBlockBlobWithMultipleUploads()
        {
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
            CreateContainer(containerName);

            var response = await client.PutBlockBlobIntelligentlyAsync(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData);

            AssertBlobOfMultipleUploads(expectedData, containerName, blobName);
            AssertIsBlockListResponse(response);
        }

        [Test]
        public void PutBlockBlobIntelligently_DataLargerThanMaxSingleUploadSize_CreatesBlockBlobWithMultipleUploads()
        {
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
            CreateContainer(containerName);

            var response = client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData);

            AssertBlobOfMultipleUploads(expectedData, containerName, blobName);
            AssertIsBlockListResponse(response);
        }

        [Test]
        public async void PutBlockBlobIntelligentlyAsync_MultipleUploadsWithContentType_CorrectContentTypeSet()
        {
            const string expectedContentType = "foo/bar";
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
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
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
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
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
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
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
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
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
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
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
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
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var expectedContentMD5 = Convert.ToBase64String(MD5.Create().ComputeHash(expectedData));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
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
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var expectedContentMD5 = Convert.ToBase64String(MD5.Create().ComputeHash(expectedData));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
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
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            const string expectedCacheControl = "ponyfoo";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
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
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            const string expectedCacheControl = "ponyfoo";
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
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
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var expectedMetadata = new Dictionary<string, string>
            {
                {"haikuLine1", "I dreamed I was in"},
                {"haikuLine2", "A long-running endless loop"},
                {"haikuLine3", "And then the next day"}
            };
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
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
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var expectedMetadata = new Dictionary<string, string>
            {
                {"haikuLine1", "I dreamed I was in"},
                {"haikuLine2", "A long-running endless loop"},
                {"haikuLine3", "And then the next day"}
            };
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings, MaxIntelligentSingleBlobUploadSizeOverride);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            CreateContainer(containerName);

            client.PutBlockBlobIntelligently(BlobServiceConstants.MaxSingleBlockUploadSize, containerName, blobName, expectedData,
                metadata: expectedMetadata);

            var metadata = GetBlobMetadata(containerName, blobName);
            AssertBlobOfSingleUpload(expectedData, containerName, blobName);
            Assert.AreEqual(expectedMetadata, metadata);
        }

        #endregion

        #endregion

        #region PutBlockBlobAsList

        // TODO Test auto-re-leasing when stream upload is available and we can control how fast they upload

        #region Stream

        [Test]
        public async void PutBlockBlobAsListAsync_StreamWithDefaultArgsOnly_CreatesBlockBlob()
        {
            var expectedData = Encoding.UTF8.GetBytes("test data");
            using (var stream = new MemoryStream(expectedData))
            {
                var containerName = GenerateSampleContainerName();
                var blobName = GenerateSampleBlobName();
                IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
                CreateContainer(containerName);

                // Tempt it to do it in two uploads by specifying half megabyte
                await client.PutBlockBlobAsListAsync(expectedData.Length - 5, containerName, blobName, stream);

                AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
                AssertBlockBlobContainsData(containerName, blobName, expectedData);
            }
        }

        #endregion

        #region Byte Array

        [Test]
        public async void PutBlockBlobAsListAsync_RequiredArgsOnly_CreatesBlockBlobFromLatestBlocks()
        {
            const string dataPerBlock = "foo";
            var expectedData = Encoding.UTF8.GetBytes(string.Format("{0}{0}{0}", dataPerBlock));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            CreateContainer(containerName);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, expectedData);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public void PutBlockBlobAsList_RequiredArgsOnly_CreatesBlockBlobFromLatestBlocks()
        {
            const string dataPerBlock = "foo";
            var expectedData = Encoding.UTF8.GetBytes(string.Format("{0}{0}{0}", dataPerBlock));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            CreateContainer(containerName);

            client.PutBlockBlobAsList(4, containerName, blobName, expectedData);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_OneByteAtATime_CreatesBlobOfCorrectLength()
        {
            const string dataPerBlock = "foo";
            var expectedData = Encoding.UTF8.GetBytes(string.Format("{0}{0}{0}", dataPerBlock));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            CreateContainer(containerName);

            await client.PutBlockBlobAsListAsync(1, containerName, blobName, expectedData);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public void PutBlockBlobAsList_OneByteAtATime_CreatesBlobOfCorrectLength()
        {
            const string dataPerBlock = "foo";
            var expectedData = Encoding.UTF8.GetBytes(string.Format("{0}{0}{0}", dataPerBlock));
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);
            CreateContainer(containerName);

            client.PutBlockBlobAsList(1, containerName, blobName, expectedData);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_LargerThanMaxSingleBlobSize_CreatesBlockBlobFromLatestBlocks()
        {
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            await client.PutBlockBlobAsListAsync(MaxIntelligentSingleBlobUploadSizeOverride / 2, containerName, blobName, expectedData);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public void PutBlockBlobAsList_LargerThanMaxSingleBlobSize_CreatesBlockBlobFromLatestBlocks()
        {
            var expectedData = new byte[MaxIntelligentSingleBlobUploadSizeOverride + 5];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            client.PutBlockBlobAsList(MaxIntelligentSingleBlobUploadSizeOverride / 2, containerName, blobName, expectedData);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public async void PutBlockBlobAsListAsync_BlockSizeLargerThanBlob_CreatesBlobWithoutError()
        {
            var expectedData = new byte[10];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            await client.PutBlockBlobAsListAsync(expectedData.Length * 2, containerName, blobName, expectedData);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        [Test]
        public void PutBlockBlobAsList_BlockSizeLargerThanBlob_CreatesBlobWithoutError()
        {
            var expectedData = new byte[10];
            var containerName = GenerateSampleContainerName();
            var blobName = GenerateSampleBlobName();
            CreateContainer(containerName);
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            client.PutBlockBlobAsList(expectedData.Length * 2, containerName, blobName, expectedData);

            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, contentType: specifiedContentType);

            var properties = GetBlobProperties(containerName, blobName);
            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, contentType: specifiedContentType);

            var properties = GetBlobProperties(containerName, blobName);
            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, contentEncoding: specifiedContentEncoding);

            var properties = GetBlobProperties(containerName, blobName);
            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, contentEncoding: specifiedContentEncoding);

            var properties = GetBlobProperties(containerName, blobName);
            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, contentLanguage: specifiedContentLanguage);

            var properties = GetBlobProperties(containerName, blobName);
            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, contentLanguage: specifiedContentLanguage);

            var properties = GetBlobProperties(containerName, blobName);
            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, cacheControl: specifiedCacheControl);

            var properties = GetBlobProperties(containerName, blobName);
            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, cacheControl: specifiedCacheControl);

            var properties = GetBlobProperties(containerName, blobName);
            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, metadata: expectedMetadata);

            var gottenMetadata = GetBlobMetadata(containerName, blobName);
            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, metadata: expectedMetadata);

            var gottenMetadata = GetBlobMetadata(containerName, blobName);
            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, contentMD5: specifiedContentMD5);

            var properties = GetBlobProperties(containerName, blobName);
            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, contentMD5: specifiedContentMD5);

            var properties = GetBlobProperties(containerName, blobName);
            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            await client.PutBlockBlobAsListAsync(4, containerName, blobName, data, contentMD5: specifiedMismatchedContentMD5);

            var properties = GetBlobProperties(containerName, blobName);
            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
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
            IBlobServiceClientEx client = new BlobServiceClientEx(AccountSettings);

            client.PutBlockBlobAsList(4, containerName, blobName, data, contentMD5: specifiedMismatchedContentMD5);

            var properties = GetBlobProperties(containerName, blobName);
            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(specifiedMismatchedContentMD5, properties.ContentMD5);
        }

        #endregion

        #endregion

        #region Assertions

        private static void AssertIsPutBlockBlobResponse(IBlobOrBlockListResponseWrapper response)
        {
            Assert.IsInstanceOf(typeof(PutBlobResponse), response);
        }

        private static void AssertIsBlockListResponse(IBlobOrBlockListResponseWrapper response)
        {
            Assert.IsInstanceOf(typeof(PutBlockListResponse), response);
        }

        private void AssertBlobOfSingleUpload(byte[] expectedData, string containerName, string blobName)
        {
            Assert.LessOrEqual(expectedData.Length, MaxIntelligentSingleBlobUploadSizeOverride);
            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        private void AssertBlobOfMultipleUploads(byte[] expectedData, string containerName, string blobName)
        {
            Assert.Greater(expectedData.Length, MaxIntelligentSingleBlobUploadSizeOverride);
            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        private void AssertBlockBlobContainsData(string containerName, string blobName, byte[] expectedData)
        {
            var client = new BlobServiceClientEx(AccountSettings);
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

    }
}
