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
    public class BlobOperationsTests
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

        #region Blob Operation Tests

        #region CopyBlob

        [Test]
        public void CopyBlob_RequiredArgsOnly_BeginsCopyOperation()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blobUri = _util.CreateBlockBlob(containerName, blobName).Uri;
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.CopyBlob(containerName, blobName, blobUri.ToString());

            _util.AssertBlobCopyOperationInProgressOrSuccessful(containerName, blobName);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void CopyBlob_EmptyContainerNameGiven_ThrowsArgumentNullException()
        {
            var blobName = _util.GenerateSampleBlobName(_runId);
            var fakeUri = "https://foo.foo.foo/";
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.CopyBlob("", blobName, fakeUri);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void CopyBlob_EmptyBlobNameGiven_ThrowsArgumentNullException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var fakeUri = "https://foo.foo.foo/";
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.CopyBlob(containerName, "", fakeUri);

            // throws exception
        }

        [Test]
        [TestCase("https://foo.foo$#@")]
        [TestCase("foo.com")]
        [TestCase("foo/foo")]
        [ExpectedException(typeof(ArgumentException))]
        public void CopyBlob_InvalidCopySourceUriGiven_ThrowsArgumentNullException(string invalidUri)
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.CopyBlob(containerName, blobName, invalidUri);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void CopyBlob_InvalidLeaseId_ThrowsArgumentException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var fakeUri = "https://foo.foo.foo/";
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.CopyBlob(containerName, blobName, fakeUri, leaseId: InvalidLeaseId);

            // throws exception
        }

        [Test]
        public void CopyBlob_MetadataGiven_CopiesBlobUsingProvidedMetadata()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blobUri = _util.CreateBlockBlob(containerName, blobName).Uri;
            var expectedMetadata = new Dictionary<string, string>{
                { "firstValue", "1" },
                { "secondValue", "2"}
            };
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.CopyBlob(containerName, blobName, blobUri.ToString(), metadata: expectedMetadata);

            var blob = _util.WaitUntilBlobCopyIsNotPending(containerName, blobName);
            Assert.AreEqual(expectedMetadata, blob.Metadata);
        }

        [Test]
        public void CopyBlob_MetadataAlreadyPresentNoneGiven_CopiesBlobUsingProvidedMetadata()
        {
            var initialMetadata = new Dictionary<string, string>{
                { "initialFirstValue", "1" },
                { "initialSecondValue", "2"}
            };
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blobUri = _util.CreateBlockBlob(containerName, blobName, metadata: initialMetadata).Uri;
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.CopyBlob(containerName, blobName, blobUri.ToString());

            var blob = _util.WaitUntilBlobCopyIsNotPending(containerName, blobName);
            Assert.AreEqual(initialMetadata, blob.Metadata);
        }

        [Test]
        public void CopyBlob_MetadataAlreadyPresentNewMetadataGiven_CopiesBlobUsingProvidedMetadata()
        {
            var initialMetadata = new Dictionary<string, string>{
                { "initialFirstValue", "1" },
                { "initialSecondValue", "2"}
            };
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blobUri = _util.CreateBlockBlob(containerName, blobName, metadata: initialMetadata).Uri;
            var expectedMetadata = new Dictionary<string, string>{
                { "expectedFirstValue", "one" },
                { "expectedSecondValue", "two"}
            };
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.CopyBlob(containerName, blobName, blobUri.ToString(), metadata: expectedMetadata);

            var blob = _util.WaitUntilBlobCopyIsNotPending(containerName, blobName);
            Assert.AreEqual(expectedMetadata, blob.Metadata);
        }

        #endregion

        #region GetBlockList

        [Test]
        public void GetBlockList_RequiredArgsOnly_GetsCommittedBlocksInTheRightOrder()
        {
            const string dataPerBlock = "foo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Committed);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            _util.PutBlockList(containerName, blobName, blockIds);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlockList(containerName, blobName, null, GetBlockListListType.All);

            Assert.Greater(response.CommittedBlocks.Count, 0);
            _util.AssertBlockListsAreEqual(containerName, blobName, BlockListingFilter.Committed, response.CommittedBlocks);
        }

        [Test]
        public async void GetBlockListAsync_RequiredArgsOnly_GetsCommittedBlocksInTheRightOrder()
        {
            const string dataPerBlock = "foo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Committed);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            _util.PutBlockList(containerName, blobName, blockIds);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = await client.GetBlockListAsync(containerName, blobName, null, GetBlockListListType.All);

            Assert.Greater(response.CommittedBlocks.Count, 0);
            _util.AssertBlockListsAreEqual(containerName, blobName, BlockListingFilter.Committed, response.CommittedBlocks);
        }

        [Test]
        public void GetBlockList_BlobWithNoCommittedBlocks_GetsEmptyETag()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Uncommitted);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, "foo");
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlockList(containerName, blobName, null, GetBlockListListType.All);

            Assert.IsNullOrEmpty(response.ETag);
        }

        [Test]
        public void GetBlockList_BlobWithCommittedBlocks_GetsPopulatedETag()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, content: "A Committed Block");
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlockList(containerName, blobName, null, GetBlockListListType.All);

            Assert.IsNotEmpty(response.ETag);
        }

        [Test]
        public void GetBlockList_BlobWithNoCommittedBlocks_GetsZeroContentLength()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Uncommitted);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, "foo");
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlockList(containerName, blobName, null, GetBlockListListType.All);

            Assert.AreEqual(0, response.BlobContentLength);
        }

        [Test]
        public void GetBlockList_BlobWithCommittedBlocks_GetsNonZeroContentLength()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, content: "A Committed Block");
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlockList(containerName, blobName, null, GetBlockListListType.All);

            Assert.Greater(response.BlobContentLength, 0);
        }

        [Test]
        public void GetBlockList_RequiredArgsOnly_GetsCorrectUncommittedBlocks()
        {
            const string dataPerBlock = "foo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Uncommitted);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlockList(containerName, blobName, null, GetBlockListListType.All);

            Assert.Greater(response.UncommittedBlocks.Count, 0);
            _util.AssertBlockListsAreEqual(containerName, blobName, BlockListingFilter.Uncommitted, response.UncommittedBlocks);
        }

        [Test]
        public async void GetBlockListAsync_RequiredArgsOnly_GetsCorrectUncommittedBlocks()
        {
            const string dataPerBlock = "foo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Uncommitted);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = await client.GetBlockListAsync(containerName, blobName, null, GetBlockListListType.All);

            Assert.Greater(response.UncommittedBlocks.Count, 0);
            _util.AssertBlockListsAreEqual(containerName, blobName, BlockListingFilter.Uncommitted, response.UncommittedBlocks);
        }

        [Test]
        public void GetBlockList_CommittedAndUncommittedBlocksExistsRequestCommitted_GetsCommittedBlocksInTheRightOrder()
        {
            const string dataPerBlock = "foo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Committed);
            var uncommittedBlockList = _util.CreateBlockIdList(3, PutBlockListListType.Uncommitted);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            var uncommittedBlockIds = _util.GetIdsFromBlockIdList(uncommittedBlockList);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            _util.PutBlockList(containerName, blobName, blockIds);
            _util.CreateBlockList(containerName, blobName, uncommittedBlockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlockList(containerName, blobName, null, GetBlockListListType.Committed);

            Assert.Greater(response.CommittedBlocks.Count, 0);
            _util.AssertBlockListsAreEqual(containerName, blobName, BlockListingFilter.Committed, response.CommittedBlocks);
        }

        [Test]
        public async void GetBlockListAsync_CommittedAndUncommittedBlocksExistsRequestCommitted_GetsCommittedBlocksInTheRightOrder()
        {
            const string dataPerBlock = "foo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Committed);
            var uncommittedBlockList = _util.CreateBlockIdList(3, PutBlockListListType.Uncommitted);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            var uncommittedBlockIds = _util.GetIdsFromBlockIdList(uncommittedBlockList);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            _util.PutBlockList(containerName, blobName, blockIds);
            _util.CreateBlockList(containerName, blobName, uncommittedBlockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = await client.GetBlockListAsync(containerName, blobName, null, GetBlockListListType.Committed);

            Assert.Greater(response.CommittedBlocks.Count, 0);
            _util.AssertBlockListsAreEqual(containerName, blobName, BlockListingFilter.Committed, response.CommittedBlocks);
        }

        [Test]
        public void GetBlockList_CommittedAndUncommittedBlocksExistsRequestUncommitted_GetsCorrectUncommittedBlocks()
        {
            const string dataPerBlock = "foo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Committed);
            var uncommittedBlockList = _util.CreateBlockIdList(3, PutBlockListListType.Uncommitted);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            var uncommittedBlockIds = _util.GetIdsFromBlockIdList(uncommittedBlockList);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            _util.PutBlockList(containerName, blobName, blockIds);
            _util.CreateBlockList(containerName, blobName, uncommittedBlockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlockList(containerName, blobName, null, GetBlockListListType.Uncommitted);

            Assert.Greater(response.UncommittedBlocks.Count, 0);
            _util.AssertBlockListsAreEqual(containerName, blobName, BlockListingFilter.Uncommitted, response.UncommittedBlocks);
        }

        [Test]
        public async void GetBlockListAsync_CommittedAndUncommittedBlocksExistsRequestUncommitted_GetsCorrectUncommittedBlocks()
        {
            const string dataPerBlock = "foo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Committed);
            var uncommittedBlockList = _util.CreateBlockIdList(3, PutBlockListListType.Uncommitted);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            var uncommittedBlockIds = _util.GetIdsFromBlockIdList(uncommittedBlockList);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            _util.PutBlockList(containerName, blobName, blockIds);
            _util.CreateBlockList(containerName, blobName, uncommittedBlockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = await client.GetBlockListAsync(containerName, blobName, null, GetBlockListListType.Uncommitted);

            Assert.Greater(response.UncommittedBlocks.Count, 0);
            _util.AssertBlockListsAreEqual(containerName, blobName, BlockListingFilter.Uncommitted, response.UncommittedBlocks);
        }

        [Test]
        public void GetBlockList_CommittedAndUncommittedBlocksExistsRequestAll_GetsAllBlocksCorrectly()
        {
            const string dataPerBlock = "foo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Committed);
            var uncommittedBlockList = _util.CreateBlockIdList(3, PutBlockListListType.Uncommitted);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            var uncommittedBlockIds = _util.GetIdsFromBlockIdList(uncommittedBlockList);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            _util.PutBlockList(containerName, blobName, blockIds);
            _util.CreateBlockList(containerName, blobName, uncommittedBlockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlockList(containerName, blobName, null, GetBlockListListType.All);

            Assert.Greater(response.UncommittedBlocks.Count, 0);
            Assert.Greater(response.CommittedBlocks.Count, 0);
            _util.AssertBlockListsAreEqual(containerName, blobName, response);
        }

        [Test]
        public async void GetBlockListAsync_CommittedAndUncommittedBlocksExistsRequestAll_GetsAllBlocksCorrectly()
        {
            const string dataPerBlock = "foo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Committed);
            var uncommittedBlockList = _util.CreateBlockIdList(3, PutBlockListListType.Uncommitted);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            var uncommittedBlockIds = _util.GetIdsFromBlockIdList(uncommittedBlockList);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            _util.PutBlockList(containerName, blobName, blockIds);
            _util.CreateBlockList(containerName, blobName, uncommittedBlockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = await client.GetBlockListAsync(containerName, blobName, null, GetBlockListListType.All);

            Assert.Greater(response.UncommittedBlocks.Count, 0);
            Assert.Greater(response.CommittedBlocks.Count, 0);
            _util.AssertBlockListsAreEqual(containerName, blobName, response);
        }

        #endregion

        #region PutBlockList

        [Test]
        public void PutBlockList_RequiredArgsOnly_CreatesBlockBlobFromLatestBlocks()
        {
            const string dataPerBlock = "foo";
            const string expectedData = "foofoofoo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            _util.AssertBlobContainsData(containerName, blobName, BlobType.BlockBlob, Encoding.Unicode.GetBytes(expectedData));
        }

        [Test]
        public async void PutBlockListAsync_RequiredArgsOnly_CreatesBlockBlobFromLatestBlocks()
        {
            const string dataPerBlock = "foo";
            const string expectedData = "foofoofoo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            _util.AssertBlobContainsData(containerName, blobName, BlobType.BlockBlob, Encoding.Unicode.GetBytes(expectedData));
        }

        [Test]
        public void PutBlockList_LeasedBlobCorrectLeaseSpecified_CreatesBlockBlobFromLatestBlocks()
        {
            const string dataPerBlock = "foo";
            const string expectedData = "foofoofoo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            var lease = _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds, leaseId: lease);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            _util.AssertBlobContainsData(containerName, blobName, BlobType.BlockBlob, Encoding.Unicode.GetBytes(expectedData));
        }

        [Test]
        public async void PutBlockListAsync_LeasedBlobCorrectLeaseSpecified_CreatesBlockBlobFromLatestBlocks()
        {
            const string dataPerBlock = "foo";
            const string expectedData = "foofoofoo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            var lease = _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds, leaseId: lease);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            _util.AssertBlobContainsData(containerName, blobName, BlobType.BlockBlob, Encoding.Unicode.GetBytes(expectedData));
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMissingAzureException))]
        public void PutBlockList_LeasedBlobWithNoLeaseGiven_ThrowsLeaseIdMissingAzureException()
        {
            const string dataPerBlock = "foo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMissingAzureException))]
        public async void PutBlockListAsync_LeasedBlobWithNoLeaseGiven_ThrowsLeaseIdMissingAzureException()
        {
            const string dataPerBlock = "foo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public void PutBlockList_LeasedBlobWithIncorrectLeaseGiven_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            const string dataPerBlock = "foo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds, leaseId: GetGuidString());

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public async void PutBlockListAsync_LeasedBlobWithIncorrectLeaseGiven_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            const string dataPerBlock = "foo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds, leaseId: GetGuidString());

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void PutBlockList_LeasedBlobWithInvalidLeaseGiven_ThrowsArgumentException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds, leaseId: InvalidLeaseId);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public async void PutBlockListAsync_LeasedBlobWithInvalidLeaseGiven_ThrowsArgumentException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds, leaseId: InvalidLeaseId);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(InvalidBlockListAzureException))]
        public void PutBlockList_InvalidBlockId_ThrowsInvalidBlockListAzureException()
        {
            const string dataPerBlock = "foo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            blockListBlockIds.Add(new BlockListBlockId
            {
                Id = Base64Converter.ConvertToBase64("id4"),
                ListType = PutBlockListListType.Latest
            });
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds);

            // Throws exception
        }

        [Test]
        [ExpectedException(typeof(InvalidBlockListAzureException))]
        public async void PutBlockListAsync_InvalidBlockId_ThrowsInvalidBlockListAzureException()
        {
            const string dataPerBlock = "foo";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            blockListBlockIds.Add(new BlockListBlockId
            {
                Id = Base64Converter.ConvertToBase64("id4"),
                ListType = PutBlockListListType.Latest
            });
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
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
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds, metadata: expectedMetadata);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
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
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds, metadata: expectedMetadata);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.IsTrue(blob.Metadata.Any(m => m.Key == "firstValue" && m.Value == "1"), "First value is missing or incorrect");
            Assert.IsTrue(blob.Metadata.Any(m => m.Key == "secondValue" && m.Value == "2"), "Second value is missing or incorrect");
        }

        [Test]
        public void PutBlockList_WithContentType_UploadsWithSpecifiedContentType()
        {
            const string dataPerBlock = "foo";
            const string expectedContentType = "text/plain";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds, contentType: expectedContentType);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentType, blob.Properties.ContentType);
        }

        [Test]
        public async void PutBlockListAsync_WithContentType_UploadsWithSpecifiedContentType()
        {
            const string dataPerBlock = "foo";
            const string expectedContentType = "text/plain";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds, contentType: expectedContentType);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentType, blob.Properties.ContentType);
        }

        [Test]
        public void PutBlockList_WithBlobContentMD5_UploadsWithSpecifiedBlobContentMD5()
        {
            const string dataPerBlock = "foo";
            const string expectedData = "foofoofoo";
            var expectedContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(Encoding.Unicode.GetBytes(expectedData)));
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds, blobContentMD5: expectedContentMD5);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentMD5, blob.Properties.ContentMD5);
        }

        [Test]
        public async void PutBlockListAsync_WithBlobContentMD5_UploadsWithSpecifiedBlobContentMD5()
        {
            const string dataPerBlock = "foo";
            const string expectedData = "foofoofoo";
            var expectedContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(Encoding.Unicode.GetBytes(expectedData)));
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds, blobContentMD5: expectedContentMD5);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentMD5, blob.Properties.ContentMD5);
        }

        [Test]
        public void PutBlockList_WithBlobContentEncoding_UploadsWithSpecifiedBlobContentEncoding()
        {
            const string dataPerBlock = "foo";
            const string expectedContentEncoding = "UTF32";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock, Encoding.UTF32);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds, contentEncoding: expectedContentEncoding);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentEncoding, blob.Properties.ContentEncoding);
        }

        [Test]
        public async void PutBlockListAsync_WithBlobContentEncoding_UploadsWithSpecifiedBlobContentEncoding()
        {
            const string dataPerBlock = "foo";
            const string expectedContentEncoding = "UTF32";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock, Encoding.UTF32);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds, contentEncoding: expectedContentEncoding);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentEncoding, blob.Properties.ContentEncoding);
        }

        [Test]
        public void PutBlockList_WithBlobContentLanguage_UploadsWithSpecifiedBlobContentLanguage()
        {
            const string dataPerBlock = "foo";
            const string expectedContentLanguage = "gibberish";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlockList(containerName, blobName, blockListBlockIds, contentLanguage: expectedContentLanguage);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentLanguage, blob.Properties.ContentLanguage);
        }

        [Test]
        public async void PutBlockListAsync_WithBlobContentLanguage_UploadsWithSpecifiedBlobContentLanguage()
        {
            const string dataPerBlock = "foo";
            const string expectedContentLanguage = "gibberish";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blockListBlockIds = _util.CreateBlockIdList(3, PutBlockListListType.Latest);
            var blockIds = _util.GetIdsFromBlockIdList(blockListBlockIds);
            _util.CreateBlockList(containerName, blobName, blockIds, dataPerBlock);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockListAsync(containerName, blobName, blockListBlockIds, contentLanguage: expectedContentLanguage);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentLanguage, blob.Properties.ContentLanguage);
        }

        #endregion

        #region PutBlockBlob

        [Test]
        public void PutBlockBlob_RequiredArgsOnly_UploadsBlobSuccessfully()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            client.PutBlockBlob(containerName, blobName, data);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        public void PutBlockBlob_EmptyBlog_UploadsBlobSuccessfully()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes(String.Empty);

            client.PutBlockBlob(containerName, blobName, data);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        public void PutBlockBlob_LeasedBlobWithCorrectLeaseIdSpecified_UploadsBlobSuccessfully()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var correctLease = _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            client.PutBlockBlob(containerName, blobName, data, leaseId: correctLease);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        public async void PutBlockBlobAsync_LeasedBlobWithCorrectLeaseId_UploadsBlobSuccessfully()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var correctLease = _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            await client.PutBlockBlobAsync(containerName, blobName, data, leaseId: correctLease);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMissingAzureException))]
        public void PutBlockBlob_LeasedBlobWithNoLeaseGiven_ThrowsLeaseIdMissingAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            client.PutBlockBlob(containerName, blobName, data);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMissingAzureException))]
        public async void PutBlockBlobAsync_LeasedBlobWithNoLeaseGiven_ThrowsLeaseIdMissingAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            await client.PutBlockBlobAsync(containerName, blobName, data);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public void PutBlockBlob_LeasedBlobWithIncorrectLeaseGiven_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            client.PutBlockBlob(containerName, blobName, data, leaseId: GetGuidString());

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public async void PutBlockBlobAsync_LeasedBlobWithIncorrectLeaseGiven_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            await client.PutBlockBlobAsync(containerName, blobName, data, leaseId: GetGuidString());

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void PutBlockBlob_LeasedBlobWithInvalidLeaseGiven_ThrowsArgumentException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            client.PutBlockBlob(containerName, blobName, data, leaseId: InvalidLeaseId);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public async void PutBlockBlobAsync_LeasedBlobWithInvalidLeaseGiven_ThrowsArgumentException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            await client.PutBlockBlobAsync(containerName, blobName, data, leaseId: InvalidLeaseId);

            // throws exception
        }

        [Test]
        public void PutBlockBlob_RequiredArgsOnlyAndBlobAlreadyExists_UploadsBlobSuccessfully()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            client.PutBlockBlob(containerName, blobName, data);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        public void PutBlockBlob_WithContentType_UploadsWithSpecifiedContentType()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");
            const string expectedContentType = "text/plain";

            client.PutBlockBlob(containerName, blobName, data, contentType: expectedContentType);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentType, blob.Properties.ContentType);
        }

        [Test]
        public void PutBlockBlob_WithContentEncoding_UploadsWithSpecifiedContentEncoding()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");
            const string expectedContentEncoding = "UTF8";

            client.PutBlockBlob(containerName, blobName, data, contentEncoding: expectedContentEncoding);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentEncoding, blob.Properties.ContentEncoding);
        }

        [Test]
        public void PutBlockBlob_WithContentLanguage_UploadsWithSpecifiedContentLanguage()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");
            const string expectedContentLanguage = "gibberish";

            client.PutBlockBlob(containerName, blobName, data, contentLanguage: expectedContentLanguage);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedContentLanguage, blob.Properties.ContentLanguage);
        }

        [Test]
        public void PutBlockBlob_WithContentMD5_UploadsWithSpecifiedContentMD5()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");
            var expectedContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(data));

            client.PutBlockBlob(containerName, blobName, data, contentMD5: expectedContentMD5);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            //this next test is not a real one, just for roundtrip verification
            Assert.AreEqual(expectedContentMD5, blob.Properties.ContentMD5);
        }

        [Test]
        [ExpectedException(typeof(Md5MismatchAzureException))]
        public void PutBlockBlob_WithIncorrectContentMD5_ThrowsMD5MismatchAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
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
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");
            const string expectedCacheControl = "123-ABC";

            client.PutBlockBlob(containerName, blobName, data, cacheControl: expectedCacheControl);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.AreEqual(expectedCacheControl, blob.Properties.CacheControl);
        }

        [Test]
        public void PutBlockBlob_WithMetadata_UploadsMetadata()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");
            var expectedMetadata = new Dictionary<string, string>(){
                { "firstValue", "1" },
                { "secondValue", "2"}
            };

            client.PutBlockBlob(containerName, blobName, data, metadata: expectedMetadata);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            Assert.IsTrue(blob.Metadata.Any(m => m.Key == "firstValue" && m.Value == "1"), "First value is missing or incorrect");
            Assert.IsTrue(blob.Metadata.Any(m => m.Key == "secondValue" && m.Value == "2"), "Second value is missing or incorrect");
        }


        [Test]
        public async Task PutBlockBlobAsync_RequiredArgsOnly_UploadsBlobSuccessfully()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            await client.PutBlockBlobAsync(containerName, blobName, data);

            _util.AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        [ExpectedException(typeof(Md5MismatchAzureException))]
        public async Task PutBlockBlobAsync_WithIncorrectContentMD5_ThrowsMD5MismatchAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
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
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var data = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlock(containerName, blobName, blockId, data);

            _util.AssertBlockExists(containerName, blobName, blockId);
        }

        [Test]
        public async void PutBlockAsync_RequiredArgsOnly_UploadsBlockSuccessfully()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var data = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockAsync(containerName, blobName, blockId, data);

            _util.AssertBlockExists(containerName, blobName, blockId);
        }

        [Test]
        public void PutBlock_LeasedBlobCorrectLeaseSpecified_UploadsBlockSuccessfully()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var blockData = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var lease = _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlock(containerName, blobName, blockId, blockData, leaseId: lease);

            _util.AssertBlockExists(containerName, blobName, blockId);
        }

        [Test]
        public async void PutBlockAsync_LeasedBlobCorrectLeaseSpecified_UploadsBlockSuccessfully()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var blockData = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var lease = _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockAsync(containerName, blobName, blockId, blockData, leaseId: lease);

            _util.AssertBlockExists(containerName, blobName, blockId);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMissingAzureException))]
        public void PutBlock_LeasedBlobWithNoLeaseGiven_ThrowsLeaseIdMissingAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var blockData = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlock(containerName, blobName, blockId, blockData);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMissingAzureException))]
        public async void PutBlockAsync_LeasedBlobWithNoLeaseGiven_ThrowsLeaseIdMissingAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var blockData = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockAsync(containerName, blobName, blockId, blockData);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public void PutBlock_LeasedBlobWithIncorrectLeaseGiven_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var blockData = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlock(containerName, blobName, blockId, blockData, leaseId: GetGuidString());

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public async void PutBlockAsync_LeasedBlobWithIncorrectLeaseGiven_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var blockData = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockAsync(containerName, blobName, blockId, blockData, leaseId: GetGuidString());

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void PutBlock_LeasedBlobWithIncorrectLeaseGiven_ThrowsArgumentException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
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
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var blockData = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockAsync(containerName, blobName, blockId, blockData, leaseId: InvalidLeaseId);

            // throws exception
        }

        [Test]
        public void PutBlock_WithContentMD5_UploadsWithSpecifiedContentMD5()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var data = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var expectedContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(data));
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var block = client.PutBlock(containerName, blobName, blockId, data, contentMD5: expectedContentMD5);

            _util.AssertBlockExists(containerName, blobName, blockId);
            Assert.AreEqual(expectedContentMD5, block.ContentMD5);
        }

        [Test]
        public async void PutBlockAsync_WithContentMD5_UploadsWithSpecifiedContentMD5()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var data = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var expectedContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(data));
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var block = await client.PutBlockAsync(containerName, blobName, blockId, data, contentMD5: expectedContentMD5);

            _util.AssertBlockExists(containerName, blobName, blockId);
            Assert.AreEqual(expectedContentMD5, block.ContentMD5);
        }

        [Test]
        [ExpectedException(typeof(Md5MismatchAzureException))]
        public void PutBlock_WithIncorrectContentMD5_ThrowsMD5MismatchAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var data = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var someOtherData = Encoding.UTF8.GetBytes("different content");
            var incorrectContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(someOtherData));
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlock(containerName, blobName, blockId, data, contentMD5: incorrectContentMD5);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(Md5MismatchAzureException))]
        public async void PutBlockAsync_WithIncorrectContentMD5_ThrowsMD5MismatchAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var data = Encoding.UTF8.GetBytes("unit test content");
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var someOtherData = Encoding.UTF8.GetBytes("different content");
            var incorrectContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(someOtherData));
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockAsync(containerName, blobName, blockId, data, contentMD5: incorrectContentMD5);

            // expects exception
        }

        [Test]
        public void PutBlock_RequiredArgsOnly_ReturnsCorrectMD5Hash()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");
            var expectedContentMD5 = Convert.ToBase64String((MD5.Create()).ComputeHash(data));

            var response = client.PutBlock(containerName, blobName, blockId, data);

            Assert.AreEqual(expectedContentMD5, response.ContentMD5);
        }

        [Test]
        public async void PutBlockAsync_RequiredArgsOnly_ReturnsCorrectMD5Hash()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            _util.CreateContainer(containerName);
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
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var fiveMegabytes = new byte[5242880];
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.PutBlock(containerName, blobName, blockId, fiveMegabytes);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public async void PutBlockAsync_TooLargePayload_ThrowsArgumentException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var fiveMegabytes = new byte[5242880];
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.PutBlockAsync(containerName, blobName, blockId, fiveMegabytes);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(InvalidBlobOrBlockAzureException))]
        public void PutBlock_DifferentLengthBlockIds_ThrowsInvalidBlobOrBlockAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var differentLengthBlockId = Base64Converter.ConvertToBase64("test-block-id-wrong-length");
            _util.CreateContainer(containerName);
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
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var blockId = Base64Converter.ConvertToBase64("test-block-id");
            var differentLengthBlockId = Base64Converter.ConvertToBase64("test-block-id-wrong-length");
            _util.CreateContainer(containerName);
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
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var blockId = Base64Converter.ConvertToBase64("test-block-id-very-long-too-long-horribly-wrong-does-not-compute-danger-will-robinson");
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var data = Encoding.UTF8.GetBytes("unit test content");

            client.PutBlock(containerName, blobName, blockId, data);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public async void PutBlockAsync_BlockIdTooLarge_ThrowsArgumentException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var blockId = Base64Converter.ConvertToBase64("test-block-id-very-long-too-long-horribly-wrong-does-not-compute-danger-will-robinson");
            _util.CreateContainer(containerName);
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
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            int expectedSize = 512;

            client.PutPageBlob(containerName, blobName, expectedSize);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.PageBlob);
            Assert.AreEqual(expectedSize, blob.Properties.Length);
        }

        [Test]
        public void PutPageBlob_WithContentType_UploadsWithSpecifiedContentType()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            string expectedContentType = "text/plain";
            int expectedSize = 512;

            client.PutPageBlob(containerName, blobName, expectedSize, contentType: expectedContentType);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.PageBlob);
            Assert.AreEqual(expectedContentType, blob.Properties.ContentType);
        }

        [Test]
        public void PutPageBlob_WithContentEncoding_UploadsWithSpecifiedContentEncoding()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            string expectedContentEncoding = "UTF8";
            int expectedSize = 512;

            client.PutPageBlob(containerName, blobName, expectedSize, contentEncoding: expectedContentEncoding);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.PageBlob);
            Assert.AreEqual(expectedContentEncoding, blob.Properties.ContentEncoding);
        }

        [Test]
        public void PutPageBlob_WithContentLanguage_UploadsWithSpecifiedContentLanguage()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            string expectedContentLanguage = "gibberish";
            int expectedSize = 512;

            client.PutPageBlob(containerName, blobName, expectedSize, contentLanguage: expectedContentLanguage);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.PageBlob);
            Assert.AreEqual(expectedContentLanguage, blob.Properties.ContentLanguage);
        }

        [Test]
        public void PutPageBlob_WithCacheControl_UploadsWithCacheControl()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            string expectedCacheControl = "123-ABC";
            int expectedSize = 512;

            client.PutPageBlob(containerName, blobName, expectedSize, cacheControl: expectedCacheControl);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.PageBlob);
            Assert.AreEqual(expectedCacheControl, blob.Properties.CacheControl);
        }

        [Test]
        public void PutPageBlob_WithMetadata_UploadsMetadata()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var expectedMetadata = new Dictionary<string, string>(){
                { "firstValue", "1" },
                { "secondValue", "2"}
            };
            const int expectedSize = 512;

            client.PutPageBlob(containerName, blobName, expectedSize, metadata: expectedMetadata);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.PageBlob);
            Assert.IsTrue(blob.Metadata.Any(m => m.Key == "firstValue" && m.Value == "1"), "First value is missing or incorrect");
            Assert.IsTrue(blob.Metadata.Any(m => m.Key == "secondValue" && m.Value == "2"), "Second value is missing or incorrect");
        }

        [Test]
        public void PutPageBlob_WithSequenceNumber_AssignsSpecifiedSequenceNumberToBlob()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            const int expectedSize = 512;
            const long expectedSequenceNumber = 123;

            client.PutPageBlob(containerName, blobName, expectedSize, sequenceNumber: expectedSequenceNumber);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.PageBlob);
            Assert.AreEqual(expectedSequenceNumber, blob.Properties.PageBlobSequenceNumber);
        }

        [Test]
        public async Task PutPageBlobAsync_WithRequiredArgs_CreatesNewPageBlob()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            int expectedSize = 512;

            await client.PutPageBlobAsync(containerName, blobName, expectedSize);

            var blob = _util.AssertBlobExists(containerName, blobName, BlobType.PageBlob);
            Assert.AreEqual(expectedSize, blob.Properties.Length);
        }

        #endregion

        #region DeleteBlob

        [Test]
        public void DeleteBlob_ExistingBlob_DeletesBlob()
        {
            var expectedContent = "Expected blob content";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, content: expectedContent);
            var client = new BlobServiceClient(AccountSettings);

            client.DeleteBlob(containerName, blobName);

            _util.AssertBlobDoesNotExist(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        public async Task DeleteBlobAsync_ExistingBlob_DeletesBlobAsynchronously()
        {
            var expectedContent = "Expected blob content";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, content: expectedContent);
            var client = new BlobServiceClient(AccountSettings);

            await client.DeleteBlobAsync(containerName, blobName);

            _util.AssertBlobDoesNotExist(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        public void DeleteBlob_LeasedBlobCorrectLeaseSpecified_DeletesBlob()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var lease = _util.LeaseBlob(containerName, blobName);
            var client = new BlobServiceClient(AccountSettings);

            client.DeleteBlob(containerName, blobName, leaseId: lease);

            _util.AssertBlobDoesNotExist(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        public async void DeleteBlobAsync_LeasedBlobCorrectLeaseSpecified_DeletesBlob()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var lease = _util.LeaseBlob(containerName, blobName);
            var client = new BlobServiceClient(AccountSettings);

            await client.DeleteBlobAsync(containerName, blobName, leaseId: lease);

            _util.AssertBlobDoesNotExist(containerName, blobName, BlobType.BlockBlob);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMissingAzureException))]
        public void DeleteBlob_LeasedBlobWithNoLeaseGiven_ThrowsLeaseIdMissingAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            var client = new BlobServiceClient(AccountSettings);

            client.DeleteBlob(containerName, blobName);

            // throw exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMissingAzureException))]
        public async void DeleteBlobAsync_LeasedBlobWithNoLeaseGiven_ThrowsLeaseIdMissingAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            var client = new BlobServiceClient(AccountSettings);

            await client.DeleteBlobAsync(containerName, blobName);

            // throw exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public void DeleteBlob_LeasedBlobWithIncorrectLeaseGiven_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            var client = new BlobServiceClient(AccountSettings);

            client.DeleteBlob(containerName, blobName, leaseId: GetGuidString());

            // throw exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public async void DeleteBlobAsync_LeasedBlobWithIncorrectLeaseGiven_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            var client = new BlobServiceClient(AccountSettings);

            await client.DeleteBlobAsync(containerName, blobName, leaseId: GetGuidString());

            // throw exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void DeleteBlob_LeasedBlobWithInvalidLeaseGiven_ThrowsArgumentException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var client = new BlobServiceClient(AccountSettings);

            client.DeleteBlob(containerName, blobName, leaseId: InvalidLeaseId);

            // throw exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public async void DeleteBlobAsync_LeasedBlobWithInvalidLeaseGiven_ThrowsArgumentException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var client = new BlobServiceClient(AccountSettings);

            await client.DeleteBlobAsync(containerName, blobName, leaseId: InvalidLeaseId);

            // throw exception
        }

        [Test]
        [ExpectedException(typeof(BlobNotFoundAzureException))]
        public void DeleteBlob_NonExistingBlob_ThrowsBlobDoesNotExistException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var client = new BlobServiceClient(AccountSettings);

            // delete blob that doesn't exist => should throw an exception
            client.DeleteBlob(containerName, blobName);
        }

        [Test]
        [ExpectedException(typeof(BlobNotFoundAzureException))]
        public async void DeleteBlobAsync_NonExistingBlob_ThrowsBlobDoesNotExistException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var client = new BlobServiceClient(AccountSettings);

            // delete blog that doesn't exist => should throw an exception
            await client.DeleteBlobAsync(containerName, blobName);
        }

        #endregion

        #region GetBlobProperties

        [Test]
        public void GetBlobProperties_ExistingBlob_GetsProperties()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.NotNull(properties);
        }

        [Test]
        public async void GetBlobPropertiesAsync_ExistingBlob_GetsProperties()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.NotNull(properties);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void GetBlobProperties_EmptyContainerName_ThrowsArgumentNullException()
        {
            var containerName = string.Empty;
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            // exception thrown
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public async void GetBlobPropertiesAsync_EmptyContainerName_ThrowsArgumentNullException()
        {
            var containerName = string.Empty;
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            // exception thrown
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void GetBlobProperties_NullContainerName_ThrowsArgumentNullException()
        {
            string containerName = null;
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            // exception thrown
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public async void GetBlobPropertiesAsync_NullContainerName_ThrowsArgumentNullException()
        {
            string containerName = null;
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            // exception thrown
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void GetBlobProperties_EmptyBlobName_ThrowsArgumentNullException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = string.Empty;
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            // exception thrown
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public async void GetBlobPropertiesAsync_EmptyBlobName_ThrowsArgumentNullException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = string.Empty;
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            // exception thrown
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void GetBlobProperties_NullBlobName_ThrowsArgumentNullException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            string blobName = null;
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            // exception thrown
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public async void GetBlobPropertiesAsync_NullBlobName_ThrowsArgumentNullException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            string blobName = null;
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            // exception thrown
        }

        [Test]
        public void GetBlobProperties_ExistingBlobJustModified_GetsCorrectLastModified()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var createdAt = DateTime.Now;
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            _util.AssertDatesEqualWithTolerance(createdAt, properties.LastModified);
        }

        [Test]
        public async void GetBlobPropertiesAsync_ExistingBlobJustModified_GetsCorrectLastModified()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var createdAt = DateTime.Now;
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            _util.AssertDatesEqualWithTolerance(createdAt, properties.LastModified);
        }

        [Test]
        public void GetBlobProperties_ExistingBlobWithMetadata_GetsCorrectMetadata()
        {
            var metadata = new Dictionary<string, string>{
                { "HeyHey", "We're the" },
                { "GroundControl", "CallingMajor" }
            };
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, metadata);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.AreEqual(metadata, properties.Metadata);
        }

        [Test]
        public async void GetBlobPropertiesAsync_ExistingBlobWithMetadata_GetsCorrectMetadata()
        {
            var metadata = new Dictionary<string, string>{
                { "HeyHey", "We're the" },
                { "GroundControl", "CallingMajor" }
            };
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, metadata);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.AreEqual(metadata, properties.Metadata);
        }

        [Test]
        public void GetBlobProperties_ExistingBlockBlob_GetsBlockBlobType()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.AreEqual(Communications.Common.BlobType.Block, properties.BlobType);
        }

        [Test]
        public async void GetBlobPropertiesAsync_ExistingBlockBlob_GetsBlockBlobType()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.AreEqual(Communications.Common.BlobType.Block, properties.BlobType);
        }

        [Test]
        public void GetBlobProperties_ExistingPageBlob_GetsPageBlobType()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreatePageBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.AreEqual(Communications.Common.BlobType.Page, properties.BlobType);
        }

        [Test]
        public async void GetBlobPropertiesAsync_ExistingPageBlob_GetsPageBlobType()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreatePageBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.AreEqual(Communications.Common.BlobType.Page, properties.BlobType);
        }

        [Test]
        public void GetBlobProperties_UnleasedBlob_GetsLeaseDurationNotSpecified()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.AreEqual(LeaseDuration.NotSpecified, properties.LeaseDuration);
        }

        [Test]
        public async void GetBlobPropertiesAsync_UnleasedBlob_GetsLeaseDurationNotSpecified()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.AreEqual(LeaseDuration.NotSpecified, properties.LeaseDuration);
        }

        [Test]
        public void GetBlobProperties_LeasedBlobWithFixedLeaseTime_GetsLeaseDurationFixed()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName, TimeSpan.FromSeconds(20));
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.AreEqual(LeaseDuration.Fixed, properties.LeaseDuration);
        }

        [Test]
        public async void GetBlobPropertiesAsync_LeasedBlobWithFixedLeaseTime_GetsLeaseDurationFixed()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName, TimeSpan.FromSeconds(20));
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.AreEqual(LeaseDuration.Fixed, properties.LeaseDuration);
        }

        [Test]
        public void GetBlobProperties_LeasedBlobWithInfiniteLeaseTime_GetsLeaseDurationInfinite()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName, null);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.AreEqual(LeaseDuration.Infinite, properties.LeaseDuration);
        }

        [Test]
        public async void GetBlobPropertiesAsync_LeasedBlobWithInfiniteLeaseTime_GetsLeaseDurationInfinite()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName, null);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.AreEqual(LeaseDuration.Infinite, properties.LeaseDuration);
        }

        [Test]
        public void GetBlobProperties_UnleasedBlob_GetsLeaseStateAvailable()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.AreEqual(LeaseState.Available, properties.LeaseState);
        }

        [Test]
        public async void GetBlobPropertiesAsync_UnleasedBlob_GetsLeaseStateAvailable()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.AreEqual(LeaseState.Available, properties.LeaseState);
        }

        [Test]
        public void GetBlobProperties_LeasedBlob_GetsLeaseStateLeased()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.AreEqual(LeaseState.Leased, properties.LeaseState);
        }

        [Test]
        public async void GetBlobPropertiesAsync_LeasedBlob_GetsLeaseStateLeased()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.AreEqual(LeaseState.Leased, properties.LeaseState);
        }

        [Test]
        public void GetBlobProperties_LeasedBlobLeaseExpired_GetsLeaseStateExpired()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName, TimeSpan.FromSeconds(15));
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            Thread.Sleep(TimeSpan.FromSeconds(16));

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.AreEqual(LeaseState.Expired, properties.LeaseState);
        }

        [Test]
        public async void GetBlobPropertiesAsync_LeasedBlobLeaseExpired_GetsLeaseStateExpired()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName, TimeSpan.FromSeconds(15));
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            Thread.Sleep(TimeSpan.FromSeconds(16));

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.AreEqual(LeaseState.Expired, properties.LeaseState);
        }

        [Test]
        public void GetBlobProperties_LeasedBlobLeaseBreaking_GetsLeaseStateBreaking()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var lease = _util.LeaseBlob(containerName, blobName, TimeSpan.FromSeconds(60));
            _util.BreakBlobLease(containerName, blobName, lease, 30);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            Thread.Sleep(TimeSpan.FromSeconds(16));

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.AreEqual(LeaseState.Breaking, properties.LeaseState);
        }

        [Test]
        public async void GetBlobPropertiesAsync_LeasedBlobLeaseBreaking_GetsLeaseStateBreaking()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var lease = _util.LeaseBlob(containerName, blobName, TimeSpan.FromSeconds(60));
            _util.BreakBlobLease(containerName, blobName, lease, 30);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            Thread.Sleep(TimeSpan.FromSeconds(16));

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.AreEqual(LeaseState.Breaking, properties.LeaseState);
        }

        [Test]
        public void GetBlobProperties_LeasedBlobLeaseBroken_GetsLeaseStateBroken()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var lease = _util.LeaseBlob(containerName, blobName, TimeSpan.FromSeconds(15));
            _util.BreakBlobLease(containerName, blobName, lease);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            Thread.Sleep(TimeSpan.FromSeconds(2));

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.AreEqual(LeaseState.Broken, properties.LeaseState);
        }

        [Test]
        public async void GetBlobPropertiesAsync_LeasedBlobLeaseBroken_GetsLeaseStateBroken()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var lease = _util.LeaseBlob(containerName, blobName, TimeSpan.FromSeconds(15));
            _util.BreakBlobLease(containerName, blobName, lease);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            Thread.Sleep(TimeSpan.FromSeconds(2));

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.AreEqual(LeaseState.Broken, properties.LeaseState);
        }

        [Test]
        public void GetBlobProperties_LeasedBlob_GetsLeaseStatusLocked()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var lease = _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.AreEqual(LeaseStatus.Locked, properties.LeaseStatus);
        }

        [Test]
        public async void GetBlobPropertiesAsync_LeasedBlob_GetsLeaseStatusLocked()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var lease = _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.AreEqual(LeaseStatus.Locked, properties.LeaseStatus);
        }

        [Test]
        public void GetBlobProperties_UnleasedBlob_GetsLeaseStatusUnlocked()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.AreEqual(LeaseStatus.Unlocked, properties.LeaseStatus);
        }

        [Test]
        public void GetBlobProperties_ExistingBlob_GetsCorrectContentLength()
        {
            const string blobContents = "foo";
            var expectedContentLength = Encoding.UTF8.GetByteCount(blobContents);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, content: blobContents);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.AreEqual(expectedContentLength, properties.ContentLength);
        }

        [Test]
        public async void GetBlobPropertiesAsync_ExistingBlob_GetsCorrectContentLength()
        {
            const string blobContents = "foo";
            var expectedContentLength = Encoding.UTF8.GetByteCount(blobContents);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, content: blobContents);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.AreEqual(expectedContentLength, properties.ContentLength);
        }

        [Test]
        public void GetBlobProperties_ExistingBlob_GetsCorrectContentType()
        {
            const string expectedContentType = "pigeon/feathers";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, contentType: expectedContentType);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.AreEqual(expectedContentType, properties.ContentType);
        }

        [Test]
        public async void GetBlobPropertiesAsync_ExistingBlob_GetsCorrectContentType()
        {
            const string expectedContentType = "pigeon/feathers";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, contentType: expectedContentType);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.AreEqual(expectedContentType, properties.ContentType);
        }

        [Test]
        public void GetBlobProperties_ExistingBlob_HasAnETag()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.IsNotNullOrEmpty(properties.ETag);
        }

        [Test]
        public async void GetBlobPropertiesAsync_ExistingBlob_HasAnETag()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.IsNotNullOrEmpty(properties.ETag);
        }

        [Test]
        public void GetBlobProperties_ExistingBlobModified_ETagPropertyChanges()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var initialProperties = _util.GetBlobProperties(containerName, blobName);
            _util.UpdateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var postChangeProperties = client.GetBlobProperties(containerName, blobName);

            Assert.AreNotEqual(initialProperties.ETag, postChangeProperties.ETag);
        }

        [Test]
        public async void GetBlobPropertiesAsync_ExistingBlobModified_ETagPropertyChanges()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var initialProperties = _util.GetBlobProperties(containerName, blobName);
            _util.UpdateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var postChangeProperties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.AreNotEqual(initialProperties.ETag, postChangeProperties.ETag);
        }

        [Test]
        public void GetBlobProperties_ExistingBlobWithSpecifiedContentMD5_GetsCorrectContentMD5()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blobMD5 = _util.CreateBlockBlob(containerName, blobName)
                .Properties.ContentMD5;
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.AreEqual(blobMD5, properties.ContentMD5);
        }

        [Test]
        public async void GetBlobPropertiesAsync_ExistingBlobWithSpecifiedContentMD5_GetsCorrectContentMD5()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var blobMD5 = _util.CreateBlockBlob(containerName, blobName)
                .Properties.ContentMD5;
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.AreEqual(blobMD5, properties.ContentMD5);
        }

        [Test]
        public void GetBlobProperties_ExistingBlobWithSpecifiedContentEncoding_GetsCorrectContentEncoding()
        {
            const string expectedEncoding = "with minimal distraction";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, contentEncoding: expectedEncoding);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.AreEqual(expectedEncoding, properties.ContentEncoding);
        }

        [Test]
        public async void GetBlobPropertiesAsync_ExistingBlobWithSpecifiedContentEncoding_GetsCorrectContentEncoding()
        {
            const string expectedEncoding = "with minimal distraction";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, contentEncoding: expectedEncoding);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.AreEqual(expectedEncoding, properties.ContentEncoding);
        }

        [Test]
        public void GetBlobProperties_ExistingBlobWithSpecifiedContentLanguage_GetsCorrectContentLanguage()
        {
            const string expectedLanguage = "dour";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, contentLanguage: expectedLanguage);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            Assert.AreEqual(expectedLanguage, properties.ContentLanguage);
        }

        [Test]
        public async void GetBlobPropertiesAsync_ExistingBlobWithSpecifiedContentLanguage_GetsCorrectContentLanguage()
        {
            const string expectedLanguage = "dour";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, contentLanguage: expectedLanguage);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            Assert.AreEqual(expectedLanguage, properties.ContentLanguage);
        }

        [Test]
        public void GetBlobProperties_ExistingBlob_GetsCorrectDate()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var createdDate = DateTime.Now;
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = client.GetBlobProperties(containerName, blobName);

            _util.AssertDatesEqualWithTolerance(createdDate, properties.Date);
        }

        [Test]
        public async void GetBlobPropertiesAsync_ExistingBlob_GetsCorrectDate()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var createdDate = DateTime.Now;
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var properties = await client.GetBlobPropertiesAsync(containerName, blobName);

            _util.AssertDatesEqualWithTolerance(createdDate, properties.Date);
        }

        [Test]
        public void GetBlobProperties_CopiedBlob_GetsCorrectCopyHeaders()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var initialBlobName = _util.GenerateSampleBlobName(_runId);
            var copiedBlobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, initialBlobName);
            _util.CopyBlob(containerName, initialBlobName, copiedBlobName);
            _util.WaitUntilBlobCopyIsNotPending(containerName, copiedBlobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlobProperties(containerName, copiedBlobName);

            _util.AssertBlobCopyPropertiesMatch(containerName, copiedBlobName, response);
        }

        [Test]
        public void GetBlobProperties_NonCopiedBlob_GetsCorrectCopyProperties()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var initialBlobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, initialBlobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlobProperties(containerName, initialBlobName);

            _util.AssertBlobCopyPropertiesMatch(containerName, initialBlobName, response);
        }

        #endregion

        #region GetBlobMetadata

        [Test]
        public void GetBlobMetadata_ExistingBlobWithMetadata_GetsMetadata()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, expectedMetadata);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlobMetadata(containerName, blobName);

            Assert.AreEqual(expectedMetadata, response.Metadata);
        }

        [Test]
        public async void GetBlobMetadataAsync_ExistingBlobWithMetadata_GetsMetadata()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, expectedMetadata);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = await client.GetBlobMetadataAsync(containerName, blobName);

            Assert.AreEqual(expectedMetadata, response.Metadata);
        }

        [Test]
        public void GetBlobMetadata_ExistingBlobWithNoMetadata_GetsEmptyMetadata()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>();
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlobMetadata(containerName, blobName);

            Assert.AreEqual(expectedMetadata, response.Metadata);
        }

        [Test]
        public async void GetBlobMetadataAsync_ExistingBlobWithNoMetadata_GetsEmptyMetadata()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>();
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = await client.GetBlobMetadataAsync(containerName, blobName);

            Assert.AreEqual(expectedMetadata, response.Metadata);
        }

        [Test]
        public void GetBlobMetadata_LeasedBlobWithMetadataCorrectLeaseProvided_GetsMetadata()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, expectedMetadata);
            var lease = _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlobMetadata(containerName, blobName, lease);

            Assert.AreEqual(expectedMetadata, response.Metadata);
        }

        [Test]
        public async void GetBlobMetadataAsync_LeasedBlobWithMetadataCorrectLeaseProvided_GetsMetadata()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, expectedMetadata);
            var lease = _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = await client.GetBlobMetadataAsync(containerName, blobName, lease);

            Assert.AreEqual(expectedMetadata, response.Metadata);
        }

        [Test]
        public void GetBlobMetadata_LeasedBlobWithMetadataNoLeaseProvided_GetsMetadata()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, expectedMetadata);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlobMetadata(containerName, blobName);

            Assert.AreEqual(expectedMetadata, response.Metadata);
        }

        [Test]
        public async void GetBlobMetadataAsync_LeasedBlobWithMetadataNoLeaseProvided_GetsMetadata()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, expectedMetadata);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = await client.GetBlobMetadataAsync(containerName, blobName);

            Assert.AreEqual(expectedMetadata, response.Metadata);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public void GetBlobMetadata_LeasedBlobWithMetadataIncorrectLeaseProvided_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, expectedMetadata);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.GetBlobMetadata(containerName, blobName, FakeLeaseId);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public async void GetBlobMetadataAsync_LeasedBlobWithMetadataIncorrectLeaseProvided_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, expectedMetadata);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.GetBlobMetadataAsync(containerName, blobName, FakeLeaseId);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void GetBlobMetadata_InvalidLeaseProvided_ThrowsArgumentException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.GetBlobMetadata(containerName, blobName, InvalidLeaseId);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public async void GetBlobMetadataAsync_InvalidLeaseProvided_ThrowsArgumentException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.GetBlobMetadataAsync(containerName, blobName, InvalidLeaseId);

            // throws exception
        }

        #endregion

        #region SetBlobMetadata

        [Test]
        public void SetBlobMetadata_ExistingBlobNoMetadata_SetsMetadata()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.SetBlobMetadata(containerName, blobName, expectedMetadata);

            _util.AssertBlobMetadata(containerName, blobName, expectedMetadata);
        }

        [Test]
        public async void SetBlobMetadataAsync_ExistingBlobNoMetadata_SetsMetadata()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.SetBlobMetadataAsync(containerName, blobName, expectedMetadata);

            _util.AssertBlobMetadata(containerName, blobName, expectedMetadata);
        }

        [Test]
        public void SetBlobMetadata_ExistingBlobExistingMetadata_OverwritesMetadata()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var oldMetadata = new Dictionary<string, string>
            {
                {"Mario", "Fire Flower"}
            };
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, oldMetadata);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.SetBlobMetadata(containerName, blobName, expectedMetadata);

            _util.AssertBlobMetadata(containerName, blobName, expectedMetadata);
        }

        [Test]
        public async void SetBlobMetadataAsync_ExistingBlobExistingMetadata_OverwritesMetadata()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var oldMetadata = new Dictionary<string, string>
            {
                {"Mario", "Fire Flower"}
            };
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, oldMetadata);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.SetBlobMetadataAsync(containerName, blobName, expectedMetadata);

            _util.AssertBlobMetadata(containerName, blobName, expectedMetadata);
        }

        [Test]
        public void SetBlobMetadata_LeasedBlobCorrectLeaseSupplied_SetsMetadata()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var lease = _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.SetBlobMetadata(containerName, blobName, expectedMetadata, lease);

            _util.AssertBlobMetadata(containerName, blobName, expectedMetadata);
        }

        [Test]
        public async void SetBlobMetadataAsync_LeasedBlobCorrectLeaseSupplied_SetsMetadata()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var lease = _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.SetBlobMetadataAsync(containerName, blobName, expectedMetadata, lease);

            _util.AssertBlobMetadata(containerName, blobName, expectedMetadata);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public void SetBlobMetadata_LeasedBlobIncorrectLeaseSupplied_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.SetBlobMetadata(containerName, blobName, expectedMetadata, FakeLeaseId);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public async void SetBlobMetadataAsync_LeasedBlobIncorrectLeaseSupplied_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.SetBlobMetadataAsync(containerName, blobName, expectedMetadata, FakeLeaseId);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithBlobOperationAzureException))]
        public void SetBlobMetadata_UnleasedBlobLeaseSupplied_ThrowsLeaseNotPresentWithBlobOperationAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.SetBlobMetadata(containerName, blobName, expectedMetadata, FakeLeaseId);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithBlobOperationAzureException))]
        public async void SetBlobMetadataAsync_UnleasedBlobLeaseSupplied_ThrowsLeaseNotPresentWithBlobOperationAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.SetBlobMetadataAsync(containerName, blobName, expectedMetadata, FakeLeaseId);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void SetBlobMetadata_InvalidLeaseSupplied_ThrowsArgumentException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.SetBlobMetadata(containerName, blobName, expectedMetadata, InvalidLeaseId);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public async void SetBlobMetadataAsync_InvalidLeaseSupplied_ThrowsArgumentException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {"CaptainAmerica", "Shield"},
                {"Thor", "Hammer"},
                {"Me", "Code"}
            };
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.SetBlobMetadataAsync(containerName, blobName, expectedMetadata, InvalidLeaseId);

            // throws exception
        }

        [Test]
        [ExpectedException(typeof(AggregateException))]
        public void SetBlobMetadata_InvalidMetadataName_ThrowsAggregateException()
        {
            const string invalidName1 = "Captain America";
            const string invalidName2 = "`Thor";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {invalidName1, "Shield"},
                {invalidName2, "Hammer"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.SetBlobMetadata(containerName, blobName, expectedMetadata);

            // throws exception
        }

        [Test]
        public void SetBlobMetadata_InvalidMetadataName_ThrowsAggregateExceptionWithOnlyInvalidNamesInExceptionList()
        {
            const string invalidName1 = "Captain America";
            const string invalidName2 = "`Thor";
            const string incorrectlyEscapedIdentifier = "if";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            var expectedMetadata = new Dictionary<string, string>
            {
                {invalidName1, "Shield"},
                {invalidName2, "Hammer"},
                {incorrectlyEscapedIdentifier, "Web"},
                {"Me", "Code"}
            };
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            try
            {
                client.SetBlobMetadata(containerName, blobName, expectedMetadata);
            }
            catch (AggregateException aggregateException)
            {
                Assert.AreEqual(3, aggregateException.InnerExceptions.Count);
                _util.AssertStringContainsString(aggregateException.InnerExceptions[0].Message, invalidName1);
                _util.AssertStringContainsString(aggregateException.InnerExceptions[1].Message, invalidName2);
                _util.AssertStringContainsString(aggregateException.InnerExceptions[2].Message, incorrectlyEscapedIdentifier);
            }
        }

        #endregion

        #region GetBlob

        [Test]
        public void GetBlob_ExistingBlob_DownloadsBlobBytes()
        {
            const string expectedContent = "Expected blob content";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlob(containerName, blobName);
            var data = response.GetDataBytes();

            Assert.AreEqual(expectedContent, Encoding.UTF8.GetString(data));
        }

        [Test]
        public async Task GetBlobAsync_ExistingBlob_DownloadsBlobBytes()
        {
            const string expectedContent = "Expected blob content";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = await client.GetBlobAsync(containerName, blobName);
            var data = response.GetDataBytes();

            Assert.AreEqual(expectedContent, Encoding.UTF8.GetString(data));
        }

        [Test]
        public void GetBlob_LeasedBlobWithCorrectLeaseSpecified_GetsBlobWithoutException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var leaseId = _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.GetBlob(containerName, blobName, null, leaseId);

            // no exception thrown
        }

        [Test]
        public async void GetBlobAsync_LeasedBlobWithCorrectLeaseSpecified_GetsBlobWithoutException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var leaseId = _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.GetBlobAsync(containerName, blobName, null, leaseId);

            // no exception thrown
        }

        [Test]
        public void GetBlob_LeasedBlobWithoutLeaseSpecified_GetsBlobWithoutException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var blob = client.GetBlob(containerName, blobName);

            blob = null;
            client = null;

            // no exception thrown
        }

        [Test]
        public async void GetBlobAsync_LeasedBlobWithoutLeaseSpecified_GetsBlobWithoutException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.GetBlobAsync(containerName, blobName);

            // no exception thrown
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public void GetBlob_LeasedBlobGivenIncorrectLease_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.GetBlob(containerName, blobName, null, leaseId: GetGuidString());

            // Throws exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithBlobOperationAzureException))]
        public async void GetBlobAsync_LeasedBlobGivenIncorrectLease_ThrowsLeaseIdMismatchWithBlobOperationAzureException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.GetBlobAsync(containerName, blobName, null, leaseId: GetGuidString());

            // Throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void GetBlob_LeasedBlobGivenInvalidLease_ThrowsArgumentException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.GetBlob(containerName, blobName, null, leaseId: InvalidLeaseId);

            // Throws exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public async void GetBlobAsync_LeasedBlobGivenInvalidLease_ThrowsArgumentException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.GetBlobAsync(containerName, blobName, null, leaseId: InvalidLeaseId);

            // Throws exception
        }

        [Test]
        public void GetBlob_ExistingBlob_DownloadsBlobStream()
        {
            const string expectedContent = "Expected blob content";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, content: expectedContent);
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
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, content: expectedContent);
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
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            client.GetBlob(containerName, blobName);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(BlobNotFoundAzureException))]
        public async Task GetBlobAsync_NonExistentBlob_ThrowsBlobDoesNotExistException()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            await client.GetBlobAsync(containerName, blobName);

            // expects exception
        }

        [Test]
        public void GetBlob_ExistingBlobByValidStartRange_DownloadBlobRangeOnly()
        {
            var expectedContent = "Expected blob content";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlob(containerName, blobName, range: new BlobRange(2));
            var data = response.GetDataBytes();

            Assert.AreEqual(expectedContent.Substring(2), Encoding.UTF8.GetString(data));
        }

        [Test]
        public async void GetBlobAsync_ExistingBlobByValidStartRange_DownloadBlobRangeOnly()
        {
            var expectedContent = "Expected blob content";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = await client.GetBlobAsync(containerName, blobName, range: new BlobRange(2));
            var data = response.GetDataBytes();

            Assert.AreEqual(expectedContent.Substring(2), Encoding.UTF8.GetString(data));
        }

        [Test]
        public void GetBlob_ExistingBlobByValidStartAndEndRange_DownloadBlobRangeOnly()
        {
            var expectedContent = "Expected blob content";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlob(containerName, blobName, range: new BlobRange(2, 6));
            var data = response.GetDataBytes();

            Assert.AreEqual(expectedContent.Substring(2, 5), Encoding.UTF8.GetString(data));
        }

        [Test]
        public async void GetBlobAsync_ExistingBlobByValidStartAndEndRange_DownloadBlobRangeOnly()
        {
            var expectedContent = "Expected blob content";
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName, content: expectedContent);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = await client.GetBlobAsync(containerName, blobName, range: new BlobRange(2, 6));
            var data = response.GetDataBytes();

            Assert.AreEqual(expectedContent.Substring(2, 5), Encoding.UTF8.GetString(data));
        }

        [Test]
        public void GetBlob_CopiedBlob_GetsCorrectCopyHeaders()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var initialBlobName = _util.GenerateSampleBlobName(_runId);
            var copiedBlobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, initialBlobName);
            _util.CopyBlob(containerName, initialBlobName, copiedBlobName);
            _util.WaitUntilBlobCopyIsNotPending(containerName, copiedBlobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlob(containerName, copiedBlobName);

            _util.AssertBlobCopyPropertiesMatch(containerName, copiedBlobName, response);
        }

        [Test]
        public void GetBlob_NonCopiedBlob_GetsCorrectCopyProperties()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var initialBlobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, initialBlobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlob(containerName, initialBlobName);

            _util.AssertBlobCopyPropertiesMatch(containerName, initialBlobName, response);
        }

        [Test]
        public void GetBlob_BlockBlob_GetsBlockBlobType()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var initialBlobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, initialBlobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlob(containerName, initialBlobName);

            Assert.AreEqual(Communications.Common.BlobType.Block, response.BlobType);
        }

        [Test]
        public void GetBlob_PageBlob_GetsBlockBlobType()
        {
            var containerName = _util.GenerateSampleContainerName(_runId);
            var initialBlobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreatePageBlob(containerName, initialBlobName);
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);

            var response = client.GetBlob(containerName, initialBlobName);

            Assert.AreEqual(Communications.Common.BlobType.Page, response.BlobType);
        }

        #endregion

        #region  LeaseBlob

        [Test]
        public void LeaseBlobAcquire_AcquireLeaseForValidBlob_AcquiresLease()
        {
            const int leaseDuration = 30;
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);

            var response = client.LeaseBlobAcquire(containerName, blobName, leaseDuration);

            _util.AssertBlobIsLeased(containerName, blobName, response.LeaseId);
        }

        [Test]
        public async void LeaseBlobAcquireAsync_AcquireLeaseForValidBlob_AcquiresLease()
        {
            const int leaseDuration = 30;
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);

            var response = await client.LeaseBlobAcquireAsync(containerName, blobName, leaseDuration);

            _util.AssertBlobIsLeased(containerName, blobName, response.LeaseId);
        }

        [Test]
        public void LeaseBlobAcquire_AcquireInfiniteLeaseForValidBlob_AcquiresLease()
        {
            const int infiniteLease = -1;
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);

            var response = client.LeaseBlobAcquire(containerName, blobName, infiniteLease);

            _util.AssertBlobIsLeased(containerName, blobName, response.LeaseId);
        }

        [Test]
        public async void LeaseBlobAcquireAsync_AcquireInfiniteLeaseForValidBlob_AcquiresLease()
        {
            const int infiniteLease = -1;
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);

            var response = await client.LeaseBlobAcquireAsync(containerName, blobName, infiniteLease);

            _util.AssertBlobIsLeased(containerName, blobName, response.LeaseId);
        }

        [Test]
        public void LeaseBlobAcquire_AcquireSpecificLeaseIdForValidBlob_AcquiresLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            var response = client.LeaseBlobAcquire(containerName, blobName, 30, expectedLeaseId);

            Assert.AreEqual(expectedLeaseId, response.LeaseId);
            _util.AssertBlobIsLeased(containerName, blobName, response.LeaseId);
        }

        [Test]
        public async void LeaseBlobAcquireAsync_AcquireSpecificLeaseIdForValidBlob_AcquiresLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            var response = await client.LeaseBlobAcquireAsync(containerName, blobName, 30, expectedLeaseId);

            Assert.AreEqual(expectedLeaseId, response.LeaseId);
            _util.AssertBlobIsLeased(containerName, blobName, response.LeaseId);
        }

        [Test]
        [ExpectedException(typeof(BlobNotFoundAzureException))]
        public void LeaseBlobAcquire_InvalidBlob_ThrowsBlobNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            _util.CreateContainer(containerName);
            var blobName = _util.GenerateSampleBlobName(_runId);

            client.LeaseBlobAcquire(containerName, blobName);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(BlobNotFoundAzureException))]
        public async void LeaseBlobAcquireAsync_InvalidBlob_ThrowsBlobNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            _util.CreateContainer(containerName);
            var blobName = _util.GenerateSampleBlobName(_runId);

            await client.LeaseBlobAcquireAsync(containerName, blobName);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(LeaseAlreadyPresentAzureException))]
        public void LeaseBlobAcquire_AlreadyLeasedBlob_ThrowsAlreadyPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);

            client.LeaseBlobAcquire(containerName, blobName);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(LeaseAlreadyPresentAzureException))]
        public async void LeaseBlobAcquireAsync_AlreadyLeasedBlob_ThrowsAlreadyPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            _util.LeaseBlob(containerName, blobName);

            await client.LeaseBlobAcquireAsync(containerName, blobName);

            // expects exception
        }

        [Test]
        public void LeaseBlobRenew_LeasedBlob_RenewsActiveLease()
        {
            var minimumWaitTime = TimeSpan.FromSeconds(15);
            var halfOfMinimum = minimumWaitTime.Subtract(TimeSpan.FromSeconds(minimumWaitTime.TotalSeconds * 0.5));
            var threeQuartersOfMinimum = minimumWaitTime.Subtract(TimeSpan.FromSeconds(minimumWaitTime.TotalSeconds * 0.75));
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var leaseId = _util.LeaseBlob(containerName, blobName, TimeSpan.FromSeconds(15));
            Thread.Sleep(halfOfMinimum);

            client.LeaseBlobRenew(containerName, blobName, leaseId);

            Thread.Sleep(threeQuartersOfMinimum); // wait again... if it didn't renew, by now it would be expired
            _util.AssertBlobIsLeased(containerName, blobName, leaseId);
        }

        [Test]
        public async void LeaseBlobRenewAsync_LeasedBlob_RenewsActiveLease()
        {
            var minimumWaitTime = TimeSpan.FromSeconds(15);
            var halfOfMinimum = minimumWaitTime.Subtract(TimeSpan.FromSeconds(minimumWaitTime.TotalSeconds * 0.5));
            var threeQuartersOfMinimum = minimumWaitTime.Subtract(TimeSpan.FromSeconds(minimumWaitTime.TotalSeconds * 0.75));
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var leaseId = _util.LeaseBlob(containerName, blobName, TimeSpan.FromSeconds(15));
            Thread.Sleep(halfOfMinimum);

            await client.LeaseBlobRenewAsync(containerName, blobName, leaseId);

            Thread.Sleep(threeQuartersOfMinimum); // wait again... if it didn't renew, by now it would be expired
            _util.AssertBlobIsLeased(containerName, blobName, leaseId);
        }

        [Test]
        public void LeaseBlobRenew_RecentlyLeasedBlob_RenewsLease()
        {
            var minimumWaitTime = TimeSpan.FromSeconds(15);
            var moreThanMinimumWaitTime = minimumWaitTime.Add(TimeSpan.FromSeconds(1));
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var leaseId = _util.LeaseBlob(containerName, blobName, TimeSpan.FromSeconds(15));
            Thread.Sleep(moreThanMinimumWaitTime);

            client.LeaseBlobRenew(containerName, blobName, leaseId);

            _util.AssertBlobIsLeased(containerName, blobName, leaseId);
        }

        [Test]
        public async void LeaseBlobRenewAsync_RecentlyLeasedBlob_RenewsLease()
        {
            var minimumWaitTime = TimeSpan.FromSeconds(15);
            var moreThanMinimumWaitTime = minimumWaitTime.Add(TimeSpan.FromSeconds(1));
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var leaseId = _util.LeaseBlob(containerName, blobName, TimeSpan.FromSeconds(15));
            Thread.Sleep(moreThanMinimumWaitTime);

            await client.LeaseBlobRenewAsync(containerName, blobName, leaseId);

            _util.AssertBlobIsLeased(containerName, blobName, leaseId);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithLeaseOperationAzureException))]
        public void LeaseBlobRenew_NonLeasedBlob_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);

            client.LeaseBlobRenew(containerName, blobName, FakeLeaseId);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithLeaseOperationAzureException))]
        public async void LeaseBlobRenewAsync_NonLeasedBlob_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);

            await client.LeaseBlobRenewAsync(containerName, blobName, FakeLeaseId);

            // expects exception
        }

        [Test]
        public void LeaseBlobChange_LeasedBlobToNewLeaseId_ChangesToMatchNewLeaseId()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var leaseId = _util.LeaseBlob(containerName, blobName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            var response = client.LeaseBlobChange(containerName, blobName, leaseId, expectedLeaseId);

            Assert.AreEqual(expectedLeaseId, response.LeaseId);
            _util.AssertBlobIsLeased(containerName, blobName, expectedLeaseId);
        }

        [Test]
        public async void LeaseBlobChangeAsync_LeasedBlobToNewLeaseId_ChangesToMatchNewLeaseId()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var leaseId = _util.LeaseBlob(containerName, blobName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            var response = await client.LeaseBlobChangeAsync(containerName, blobName, leaseId, expectedLeaseId);

            Assert.AreEqual(expectedLeaseId, response.LeaseId);
            _util.AssertBlobIsLeased(containerName, blobName, expectedLeaseId);
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithLeaseOperationAzureException))]
        public void LeaseBlobChange_NonLeasedBlob_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            client.LeaseBlobChange(containerName, blobName, FakeLeaseId, expectedLeaseId);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithLeaseOperationAzureException))]
        public async void LeaseBlobChangeAsync_NonLeasedBlob_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            await client.LeaseBlobChangeAsync(containerName, blobName, FakeLeaseId, expectedLeaseId);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(BlobNotFoundAzureException))]
        public void LeaseBlobChange_NonexistentBlob_ThrowsBlobNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            client.LeaseBlobChange(containerName, blobName, FakeLeaseId, expectedLeaseId);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(BlobNotFoundAzureException))]
        public async void LeaseBlobChangeAsync_NonexistentBlob_ThrowsBlobNotFoundException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            var expectedLeaseId = Guid.NewGuid().ToString();

            await client.LeaseBlobChangeAsync(containerName, blobName, FakeLeaseId, expectedLeaseId);

            // expects exception
        }

        [Test]
        public void LeaseBlobRelease_LeasedBlob_ReleasesLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var leaseId = _util.LeaseBlob(containerName, blobName);

            client.LeaseBlobRelease(containerName, blobName, leaseId);

            _util.AssertBlobIsNotLeased(containerName, blobName);
        }

        [Test]
        public async void LeaseBlobReleaseAsync_LeasedBlob_ReleasesLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var leaseId = _util.LeaseBlob(containerName, blobName);

            await client.LeaseBlobReleaseAsync(containerName, blobName, leaseId);

            _util.AssertBlobIsNotLeased(containerName, blobName);
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithLeaseOperationAzureException))]
        public void LeaseBlobRelease_NonLeasedBlob_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);

            client.LeaseBlobRelease(containerName, blobName, FakeLeaseId);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(LeaseIdMismatchWithLeaseOperationAzureException))]
        public async void LeaseBlobReleaseAsync_NonLeasedBlob_ThrowsLeaseIdMismatchException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);

            await client.LeaseBlobReleaseAsync(containerName, blobName, FakeLeaseId);

            // expects exception
        }

        [Test]
        public void LeaseBlobBreak_LeasedBlob_BreaksLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var leaseId = _util.LeaseBlob(containerName, blobName);

            client.LeaseBlobBreak(containerName, blobName, leaseId, 0);

            var leaseState = _util.GetBlobLeaseState(containerName, blobName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.LeaseState.Broken, leaseState);
        }

        [Test]
        public async void LeaseBlobBreakAsync_LeasedBlob_BreaksLease()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var leaseId = _util.LeaseBlob(containerName, blobName);

            await client.LeaseBlobBreakAsync(containerName, blobName, leaseId, 0);

            var leaseState = _util.GetBlobLeaseState(containerName, blobName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.LeaseState.Broken, leaseState);
        }

        [Test]
        public void LeaseBlobBreak_LeasedBlobWithLongBreakPeriod_SetLeaseToBreaking()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var leaseId = _util.LeaseBlob(containerName, blobName);

            client.LeaseBlobBreak(containerName, blobName, leaseId, 60);

            var leaseState = _util.GetBlobLeaseState(containerName, blobName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.LeaseState.Breaking, leaseState);
        }

        [Test]
        public async void LeaseBlobBreakAsync_LeasedBlobWithLongBreakPeriod_SetLeaseToBreaking()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);
            var leaseId = _util.LeaseBlob(containerName, blobName);

            await client.LeaseBlobBreakAsync(containerName, blobName, leaseId, 60);

            var leaseState = _util.GetBlobLeaseState(containerName, blobName);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Blob.LeaseState.Breaking, leaseState);
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithLeaseOperationAzureException))]
        public void LeaseBlobBreak_NonLeasedBlob_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);

            client.LeaseBlobBreak(containerName, blobName, FakeLeaseId, 0);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(LeaseNotPresentWithLeaseOperationAzureException))]
        public async void LeaseBlobBreakAsync_NonLeasedBlob_ThrowsLeaseNotPresentException()
        {
            IBlobServiceClient client = new BlobServiceClient(AccountSettings);
            var containerName = _util.GenerateSampleContainerName(_runId);
            var blobName = _util.GenerateSampleBlobName(_runId);
            _util.CreateContainer(containerName);
            _util.CreateBlockBlob(containerName, blobName);

            await client.LeaseBlobBreakAsync(containerName, blobName, FakeLeaseId, 0);

            // expects exception
        }

        #endregion

        #endregion

    }
}
