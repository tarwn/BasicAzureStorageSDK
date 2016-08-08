using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Tests.Integration.BlobServiceClientTests
{
    public class BlobUtil
    {
        public CloudStorageAccount _storageAccount;
        private readonly Dictionary<string, string> _containersToCleanUp = new Dictionary<string, string>();

        public BlobUtil(string connectionString)
        {
            _storageAccount = CloudStorageAccount.Parse(connectionString);
        }

        public string GenerateSampleContainerName(string runId)
        {
            var name = string.Format("unit-test-{0}-{1}", runId, Guid.NewGuid()).ToLower();
            RegisterContainerForCleanup(name, null);
            return name;
        }

        public string GenerateSampleBlobName(string runId)
        {
            return string.Format("unit-test-{0}-{1}", runId, Guid.NewGuid());
        }

        public void RegisterContainerForCleanup(string containerName, string leaseId)
        {
            _containersToCleanUp[containerName] = leaseId;
        }

        #region Assertions

        public void AssertStringContainsString(string wholeString, string expectedSubString)
        {
            Assert.True(wholeString.Contains(expectedSubString), "Ensuring {0} contains {1}", wholeString, expectedSubString);
        }

        public void AssertDatesEqualWithTolerance(DateTime expected, DateTime actual, int secondTolerance = 10)
        {
            Assert.LessOrEqual(expected.Subtract(actual).TotalSeconds, secondTolerance);
        }

        public void AssertContainerExists(string containerName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail(String.Format("AssertContainerExists: The container '{0}' does not exist", containerName));
        }

        public void AssertContainerDoesNotExist(string containerName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (container.Exists())
                Assert.Fail(String.Format("AssertContainerDoesNotExist: The container '{0}' exists", containerName));
        }

        public void AssertContainerIsLeased(string containerName, string leaseId)
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

        public void AssertContainerIsNotLeased(string containerName)
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

        public void AssertContainerAccess(string containerName, BlobContainerPublicAccessType containerAccessType)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail(String.Format("AssertContainerAccess: The container '{0}' does not exist", containerName));

            var permissions = container.GetPermissions();
            Assert.AreEqual(containerAccessType, permissions.PublicAccess, String.Format("AssertContainerAccess: Container access was expected to be {0}, but it is actually {1}", containerAccessType, permissions.PublicAccess));
        }

        public ICloudBlob AssertBlobExists(string containerName, string blobName, BlobType blobType)
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

        public void AssertBlobCopyOperationInProgressOrSuccessful(string containerName, string blobName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail("AssertBlobCopyOperationInProgressOrSuccessful: The container '{0}' does not exist", containerName);

            var blob = container.GetBlobReferenceFromServer(blobName);

            if (!blob.Exists())
                Assert.Fail("AssertBlobCopyOperationInProgressOrSuccessful: The blob '{0}' does not exist", blobName);

            Assert.True(blob.CopyState.Status.HasFlag(CopyStatus.Pending) || blob.CopyState.Status.HasFlag(CopyStatus.Success));
        }

        public IDictionary<string, string> AssertBlobMetadata(string containerName, string blobName, Dictionary<string, string> expectedMetadata)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail("AssertBlobExists: The container '{0}' does not exist", containerName);

            var blob = container.GetBlobReferenceFromServer(blobName);

            if (!blob.Exists())
                Assert.Fail("AssertBlobExists: The blob '{0}' does not exist", blobName);

            Assert.AreEqual(expectedMetadata, blob.Metadata);

            return blob.Metadata;
        }

        public void AssertBlobIsLeased(string containerName, string blobName, string leaseId)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail("AssertBlobIsLeased: The container '{0}' does not exist", containerName);

            var blob = container.GetBlockBlobReference(blobName);
            if (!blob.Exists())
                Assert.Fail("AssertBlobIsLeased: The blob '{0}' does not exist", blobName);

            try
            {
                blob.RenewLease(new AccessCondition { LeaseId = leaseId });
            }
            catch (Exception exc)
            {
                Assert.Fail("AssertBlobIsLeased: The blob '{0}' gave an {1} exception when renewing with the specified lease id: {2}", blobName, exc.GetType().Name, exc.Message);
            }
        }

        public void AssertBlobIsNotLeased(string containerName, string blobName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail("AssertBlobIsNotLeased: The container '{0}' does not exist", containerName);

            var blob = container.GetBlockBlobReference(blobName);
            if (!blob.Exists())
                Assert.Fail("AssertBlobIsNotLeased: The blob '{0}' does not exist", blobName);

            try
            {
                blob.AcquireLease(null, null);
            }
            catch (Exception exc)
            {
                Assert.Fail("AssertBlobIsNotLeased: The blob '{0}' gave an {1} exception when attempting to acquire a new lease: {2}", blobName, exc.GetType().Name, exc.Message);
            }
        }

        public ListBlockItem AssertBlockExists(string containerName, string blobName, string blockId, BlockListingFilter blockType = BlockListingFilter.All)
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

        public List<ListBlockItem> AssertBlockListsAreEqual(string containerName, string blobName, Basic.Azure.Storage.Communications.BlobService.BlobOperations.GetBlockListResponse response)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail("AssertBlockExists: The container '{0}' does not exist", containerName);

            var blob = container.GetBlockBlobReference(blobName);
            var committedBlockList = blob.DownloadBlockList(BlockListingFilter.Committed);
            var uncommittedBlockList = blob.DownloadBlockList(BlockListingFilter.Uncommitted);
            var blockList = committedBlockList.Concat(uncommittedBlockList).ToList();

            var gottenBlocks = response.CommittedBlocks.Concat(response.UncommittedBlocks).ToList();
            var gottenBlocksCount = gottenBlocks.Count;
            Assert.AreEqual(blockList.Count, gottenBlocksCount);
            for (var i = 0; i < gottenBlocksCount; i++)
            {
                var expectedBlock = blockList[i];
                var gottenBlock = gottenBlocks[i];
                Assert.AreEqual(expectedBlock.Name, gottenBlock.Name);
                Assert.AreEqual(expectedBlock.Length, gottenBlock.Size);
            }

            return blockList;
        }
        public List<ListBlockItem> AssertBlockListsAreEqual(string containerName, string blobName, BlockListingFilter blockType, List<Basic.Azure.Storage.Communications.BlobService.ParsedBlockListBlockId> gottenBlocks)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail("AssertBlockExists: The container '{0}' does not exist", containerName);

            var blob = container.GetBlockBlobReference(blobName);
            var blockList = blob.DownloadBlockList(blockType).ToList();

            Assert.AreEqual(blockList.Count, gottenBlocks.Count);
            for (var i = 0; i < gottenBlocks.Count; i++)
            {
                var expectedBlock = blockList[i];
                var gottenBlock = gottenBlocks[i];
                Assert.AreEqual(expectedBlock.Name, gottenBlock.Name);
                Assert.AreEqual(expectedBlock.Length, gottenBlock.Size);
            }

            return blockList;
        }

        public ICloudBlob AssertBlobDoesNotExist(string containerName, string blobName, BlobType blobType)
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

        public ICloudBlob AssertBlobContainsData(string containerName, string blobName, BlobType blobType, byte[] expectedData)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail("AssertBlobContainsData: The container '{0}' does not exist", containerName);

            var blob = (blobType == BlobType.BlockBlob
                ? (ICloudBlob)container.GetBlockBlobReference(blobName)
                : (ICloudBlob)container.GetPageBlobReference(blobName));

            if (!blob.Exists())
                Assert.Fail("AssertBlobContainsData: The blob '{0}' does not exist", blobName);

            using (var stream = new MemoryStream())
            {
                blob.DownloadToStream(stream);

                var gottenData = stream.ToArray();

                // Comparing strings -> MUCH faster than comparing the raw arrays
                var gottenDataString = Convert.ToBase64String(gottenData);
                var expectedDataString = Convert.ToBase64String(expectedData);

                Assert.AreEqual(expectedData.Length, gottenData.Length);
                Assert.AreEqual(gottenDataString, expectedDataString);
            }

            return blob;
        }

        public void AssertBlobCopyPropertiesMatch(string containerName, string blobName, Basic.Azure.Storage.Communications.BlobService.BlobOperations.GetBlobResponse response)
        {
            AssertBlobCopyPropertiesMatch(containerName, blobName, response.CopyStatus, response.CopyProgress, response.CopyCompletionTime, response.CopyStatusDescription, response.CopyId, response.CopySource);
        }

        public void AssertBlobCopyPropertiesMatch(string containerName, string blobName, Basic.Azure.Storage.Communications.BlobService.BlobOperations.GetBlobPropertiesResponse response)
        {
            AssertBlobCopyPropertiesMatch(containerName, blobName, response.CopyStatus, response.CopyProgress, response.CopyCompletionTime, response.CopyStatusDescription, response.CopyId, response.CopySource);
        }
        public void AssertBlobCopyPropertiesMatch(string containerName, string blobName, Communications.Common.CopyStatus? copyStatus, Basic.Azure.Storage.Communications.BlobService.BlobCopyProgress copyProgress, DateTime? copyCompletionTime, string copyStatusDescription, string copyId, string copySource)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail("AssertBlobCopyPropertiesMatch: The container '{0}' does not exist", containerName);

            var blob = container.GetBlobReferenceFromServer(blobName);
            if (!blob.Exists())
                Assert.Fail("AssertBlobCopyPropertiesMatch: The blob '{0}' does not exist", blobName);

            var copyState = blob.CopyState;

            if (null == copyState)
            {
                Assert.IsNull(copyStatus);
                Assert.IsNull(copyProgress);
                Assert.IsNull(copyCompletionTime);
                Assert.IsNull(copyStatusDescription);
                Assert.IsNull(copyId);
                Assert.IsNull(copySource);
            }
            else
            {
                Assert.AreEqual(copyState.Status.ToString(), copyStatus.ToString());
                Assert.AreEqual(copyState.BytesCopied, copyProgress.BytesCopied);
                Assert.AreEqual(copyState.TotalBytes, copyProgress.BytesTotal);
                Assert.AreEqual(copyState.CompletionTime.Value.LocalDateTime, copyCompletionTime.Value);
                Assert.AreEqual(copyState.StatusDescription, copyStatusDescription);
                Assert.AreEqual(copyState.CopyId, copyId);
                Assert.AreEqual(copyState.Source, copySource);
            }
        }

        public IDictionary<string, string> GetContainerMetadata(string containerName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);

            container.FetchAttributes();
            return container.Metadata;
        }

        public BlobProperties GetBlobProperties(string containerName, string blobName)
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

        public IDictionary<string, string> GetBlobMetadata(string containerName, string blobName)
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

        public void AssertIdentifierInSharedAccessPolicies(SharedAccessBlobPolicies sharedAccessPolicies, Basic.Azure.Storage.Communications.Common.BlobSignedIdentifier expectedIdentifier, SharedAccessBlobPermissions permissions)
        {
            var policy = sharedAccessPolicies.Where(i => i.Key.Equals(expectedIdentifier.Id, StringComparison.InvariantCultureIgnoreCase)).FirstOrDefault();
            Assert.IsNotNull(policy);
            Assert.AreEqual(expectedIdentifier.AccessPolicy.StartTime, policy.Value.SharedAccessStartTime.Value.UtcDateTime);
            Assert.AreEqual(expectedIdentifier.AccessPolicy.Expiry, policy.Value.SharedAccessExpiryTime.Value.UtcDateTime);
            Assert.IsTrue(policy.Value.Permissions.HasFlag(permissions));
        }

        public void AssertIsPutBlockBlobResponse(Basic.Azure.Storage.Extensions.Contracts.IBlobOrBlockListResponseWrapper response)
        {
            Assert.IsInstanceOf(typeof(Basic.Azure.Storage.Communications.BlobService.BlobOperations.PutBlobResponse), response);
        }

        public void AssertIsBlockListResponse(Basic.Azure.Storage.Extensions.Contracts.IBlobOrBlockListResponseWrapper response)
        {
            Assert.IsInstanceOf(typeof(Basic.Azure.Storage.Communications.BlobService.BlobOperations.PutBlockListResponse), response);
        }

        public void AssertBlobOfSingleUpload(byte[] expectedData, string containerName, string blobName, int maxIntelligentSingleBlobUploadSizeOverride)
        {
            Assert.LessOrEqual(expectedData.Length, maxIntelligentSingleBlobUploadSizeOverride);
            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }

        public void AssertBlobOfMultipleUploads(byte[] expectedData, string containerName, string blobName, int maxIntelligentSingleBlobUploadSizeOverride)
        {
            Assert.Greater(expectedData.Length, maxIntelligentSingleBlobUploadSizeOverride);
            AssertBlobExists(containerName, blobName, BlobType.BlockBlob);
            AssertBlockBlobContainsData(containerName, blobName, expectedData);
        }


        //TODO rewrite without using library under test to validate 
        public void AssertBlockBlobContainsData(string containerName, string blobName, byte[] expectedData)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail("AssertBlobContainsData: The container '{0}' does not exist", containerName);

            var blob = container.GetBlockBlobReference(blobName);

            if (!blob.Exists())
                Assert.Fail("AssertBlobContainsData: The blob '{0}' does not exist", blobName);

            using (var stream = new MemoryStream())
            {
                blob.DownloadToStream(stream);

                var gottenData = stream.ToArray();

                // Comparing strings -> MUCH faster than comparing the raw arrays
                var gottenDataString = Encoding.Unicode.GetString(gottenData);
                var expectedDataString = Encoding.Unicode.GetString(expectedData);

                Assert.AreEqual(expectedData.Length, gottenData.Length);
                Assert.AreEqual(expectedData.Length, gottenData.Length);
                Assert.AreEqual(gottenDataString, expectedDataString);
            }
        }

        #endregion

        #region Setup Mechanics

        public void CreateContainer(string containerName, Dictionary<string, string> metadata = null)
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

        public string LeaseBlob(string containerName, string blobName, TimeSpan? leaseTime = null, string leaseId = null)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail("LeaseBlob: The container '{0}' does not exist", containerName);

            var blob = container.GetBlockBlobReference(blobName);

            if (!blob.Exists())
                Assert.Fail("LeaseBlob: The blob '{0}' does not exist", blobName);

            return blob.AcquireLease(leaseTime, leaseId);
        }

        public string LeaseContainer(string containerName, TimeSpan? leaseTime, string leaseId)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var lease = container.AcquireLease(leaseTime, leaseId);
            RegisterContainerForCleanup(containerName, lease);
            return lease;
        }

        public void ReleaseContainerLease(string containerName, string lease)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            container.ReleaseLease(new AccessCondition() { LeaseId = lease });
        }

        public void BreakContainerLease(string containerName, string lease, int breakPeriod = 1)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            container.BreakLease(TimeSpan.FromSeconds(breakPeriod), new AccessCondition() { LeaseId = lease });
        }

        public void AddContainerAccessPolicy(string containerName, BlobContainerPublicAccessType publicAccess, string id = null, DateTime? startDate = null, DateTime? expiry = null)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var permissions = container.GetPermissions();
            permissions.PublicAccess = publicAccess;
            if (!string.IsNullOrEmpty(id) && startDate.HasValue && expiry.HasValue)
            {
                permissions.SharedAccessPolicies.Add(id, new SharedAccessBlobPolicy()
                {
                    Permissions = SharedAccessBlobPermissions.Read,
                    SharedAccessStartTime = startDate,
                    SharedAccessExpiryTime = expiry
                });
            }
            container.SetPermissions(permissions);
        }

        public BlobContainerPermissions GetContainerPermissions(string containerName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var permissions = container.GetPermissions();
            return permissions;
        }

        public LeaseState GetContainerLeaseState(string containerName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            container.FetchAttributes();
            return container.Properties.LeaseState;
        }

        public LeaseState GetBlobLeaseState(string containerName, string blobName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetBlobReferenceFromServer(blobName);
            blob.FetchAttributes();
            return blob.Properties.LeaseState;
        }

        public List<ListBlockItem> CreateBlockList(string containerName, string blobName,
            IEnumerable<string> blockIdsToCreate, string dataPerBlock, Encoding encoder = null)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);

            byte[] data = (encoder ?? Encoding.Unicode).GetBytes(dataPerBlock);
            // non-Base64 values fail?
            foreach (var curBlockId in blockIdsToCreate)
            {
                blob.PutBlock(curBlockId, new MemoryStream(data), null);
            }

            return blob
                .DownloadBlockList(BlockListingFilter.All)
                .ToList();
        }

        public void PutBlockList(string containerName, string blobName, IEnumerable<string> blockIds)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);

            blob.PutBlockList(blockIds);
        }

        public Basic.Azure.Storage.Communications.BlobService.BlockListBlockIdList CreateBlockIdList(int idCount, Basic.Azure.Storage.Communications.BlobService.PutBlockListListType listType)
        {
            var idList = new Basic.Azure.Storage.Communications.BlobService.BlockListBlockIdList();
            for (var i = 0; i < idCount; i++)
            {
                idList.Add(new Basic.Azure.Storage.Communications.BlobService.BlockListBlockId
                {
                    Id = Basic.Azure.Storage.Communications.Utility.Base64Converter.ConvertToBase64(Guid.NewGuid().ToString()),
                    ListType = listType
                });
            }
            return idList;
        }

        public List<string> GetIdsFromBlockIdList(Basic.Azure.Storage.Communications.BlobService.BlockListBlockIdList list)
        {
            return list.Select(bid => bid.Id).ToList();
        }

        public void BreakBlobLease(string containerName, string blobName, string lease, int breakPeriod = 1)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetBlobReferenceFromServer(blobName);
            blob.BreakLease(TimeSpan.FromSeconds(breakPeriod), new AccessCondition { LeaseId = lease });
        }

        public CloudBlockBlob CreateBlockBlob(string containerName, string blobName, Dictionary<string, string> metadata = null, string content = "Generic content", string contentType = "", string contentEncoding = "", string contentLanguage = "")
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);

            byte[] data = UTF8Encoding.UTF8.GetBytes(content);
            blob.UploadFromByteArray(data, 0, data.Length);

            blob.Properties.ContentType = contentType;
            blob.Properties.ContentEncoding = contentEncoding;
            blob.Properties.ContentLanguage = contentLanguage;
            blob.SetProperties();

            if (metadata != null)
            {
                foreach (var key in metadata.Keys)
                {
                    blob.Metadata.Add(key, metadata[key]);
                }
                blob.SetMetadata();
            }

            return blob;
        }

        public void UpdateBlockBlob(string containerName, string blobName, Dictionary<string, string> metadata = null, string content = "Generic content", string contentType = "", string contentEncoding = "", string contentLanguage = "")
        {
            CreateBlockBlob(containerName, blobName, metadata, content, contentType, contentEncoding, contentLanguage);
        }

        public void CreatePageBlob(string containerName, string blobName, Dictionary<string, string> metadata = null)
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

        public void CreateBlobUncommitted(string containerName, string blobName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);

            byte[] data = UTF8Encoding.UTF8.GetBytes("Generic content");
            // non-Base64 values fail?
            var blockId = Convert.ToBase64String(Encoding.UTF8.GetBytes("A"));
            blob.PutBlock(blockId, new MemoryStream(data), null);
        }

        public void CopyBlob(string containerName, string sourceBlobName, string targetBlobName)
        {
            // could we have made this require more work?
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var sourceBlob = container.GetBlockBlobReference(sourceBlobName);
            var targetBlob = container.GetBlockBlobReference(targetBlobName);
            targetBlob.StartCopyFromBlob(sourceBlob);
        }

        public void SnapshotBlob(string containerName, string blobName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);
            blob.CreateSnapshot();
        }

        public ICloudBlob WaitUntilBlobCopyIsNotPending(string containerName, string blobName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetBlobReferenceFromServer(blobName);

            Task.Run(() =>
            {
                blob.FetchAttributes();
                while (blob.CopyState.Status == CopyStatus.Pending)
                {
                    Thread.Sleep(50);
                }
            }).GetAwaiter().GetResult();

            return blob;
        }


        #endregion


        #region Cleanup
        public void Cleanup()
        {
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
                    catch
                    {
                        // ignore
                    }
                }
                container.DeleteIfExists();
            }
        }
        #endregion
    }
}
