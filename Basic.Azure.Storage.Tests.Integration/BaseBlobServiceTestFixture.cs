using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.Common;
using Microsoft.WindowsAzure.Storage;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Basic.Azure.Storage.Communications.Utility;
using Microsoft.WindowsAzure.Storage.Blob;
using BlobType = Microsoft.WindowsAzure.Storage.Blob.BlobType;
using System.Configuration;

namespace Basic.Azure.Storage.Tests.Integration
{
    [TestFixture]
    public class BaseBlobServiceClientTestFixture
    {
        protected const string InvalidLeaseId = "InvalidLeaseId";

        private readonly string _azureConnectionString = ConfigurationManager.AppSettings["AzureConnectionString"];
        private readonly string _runId = DateTime.Now.ToString("yyyy-MM-dd");
        private readonly Dictionary<string, string> _containersToCleanUp = new Dictionary<string, string>();

        protected StorageAccountSettings AccountSettings
        {
            get
            {
                return StorageAccountSettings.Parse(_azureConnectionString);
            }
        }

        private CloudStorageAccount StorageAccount
        {
            get
            {
                return CloudStorageAccount.Parse(_azureConnectionString);
            }
        }

        protected string GenerateSampleContainerName()
        {
            var name = string.Format("unit-test-{0}-{1}", _runId, Guid.NewGuid()).ToLower();
            RegisterContainerForCleanup(name, null);
            return name;
        }

        protected string GenerateSampleBlobName()
        {
            return string.Format("unit-test-{0}-{1}", _runId, Guid.NewGuid());
        }

        protected void RegisterContainerForCleanup(string containerName, string leaseId)
        {
            _containersToCleanUp[containerName] = leaseId;
        }

        protected static string FakeLeaseId { get { return "a28cf439-8776-4653-9ce8-4e3df49b4a72"; } }

        [TestFixtureTearDown]
        public void TestFixtureTeardown()
        {
            //let's clean up!
            var client = StorageAccount.CreateCloudBlobClient();
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

        #region Assertions

        protected void AssertDatesEqualWithTolerance(DateTime expected, DateTime actual, int secondTolerance = 10)
        {
            Assert.LessOrEqual(expected.Subtract(actual).TotalSeconds, secondTolerance);
        }

        protected void AssertContainerExists(string containerName)
        {
            var client = StorageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail(String.Format("AssertContainerExists: The container '{0}' does not exist", containerName));
        }

        protected void AssertContainerDoesNotExist(string containerName)
        {
            var client = StorageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (container.Exists())
                Assert.Fail(String.Format("AssertContainerDoesNotExist: The container '{0}' exists", containerName));
        }

        protected void AssertContainerIsLeased(string containerName, string leaseId)
        {
            var client = StorageAccount.CreateCloudBlobClient();
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

        protected void AssertContainerIsNotLeased(string containerName)
        {
            var client = StorageAccount.CreateCloudBlobClient();
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

        protected void AssertContainerAccess(string containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType containerAccessType)
        {
            var client = StorageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail(String.Format("AssertContainerAccess: The container '{0}' does not exist", containerName));

            var permissions = container.GetPermissions();
            Assert.AreEqual(containerAccessType, permissions.PublicAccess, String.Format("AssertContainerAccess: Container access was expected to be {0}, but it is actually {1}", containerAccessType, permissions.PublicAccess));
        }

        protected Microsoft.WindowsAzure.Storage.Blob.ICloudBlob AssertBlobExists(string containerName, string blobName, BlobType blobType)
        {
            var client = StorageAccount.CreateCloudBlobClient();
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

        protected void AssertBlobIsLeased(string containerName, string blobName, string leaseId)
        {
            var client = StorageAccount.CreateCloudBlobClient();
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

        protected void AssertBlobIsNotLeased(string containerName, string blobName)
        {
            var client = StorageAccount.CreateCloudBlobClient();
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

        protected Microsoft.WindowsAzure.Storage.Blob.ListBlockItem AssertBlockExists(string containerName, string blobName, string blockId, BlockListingFilter blockType = BlockListingFilter.All)
        {
            var client = StorageAccount.CreateCloudBlobClient();
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

        protected Microsoft.WindowsAzure.Storage.Blob.ICloudBlob AssertBlobDoesNotExist(string containerName, string blobName, BlobType blobType)
        {
            var client = StorageAccount.CreateCloudBlobClient();
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

        protected Microsoft.WindowsAzure.Storage.Blob.ICloudBlob AssertBlobContainsData(string containerName, string blobName, BlobType blobType, byte[] expectedData)
        {
            var client = StorageAccount.CreateCloudBlobClient();
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

        protected IDictionary<string, string> GetContainerMetadata(string containerName)
        {
            var client = StorageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);

            container.FetchAttributes();
            return container.Metadata;
        }

        protected BlobProperties GetBlobProperties(string containerName, string blobName)
        {
            var client = StorageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail("GetBlobProperties: The container '{0}' does not exist", containerName);

            var blob = container.GetBlockBlobReference(blobName);

            if (!blob.Exists())
                Assert.Fail("GetBlobProperties: The blob '{0}' does not exist", blobName);

            return blob.Properties;
        }

        protected IDictionary<string, string> GetBlobMetadata(string containerName, string blobName)
        {
            var client = StorageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail("GetBlobProperties: The container '{0}' does not exist", containerName);

            var blob = container.GetBlockBlobReference(blobName);

            if (!blob.Exists())
                Assert.Fail("GetBlobProperties: The blob '{0}' does not exist", blobName);

            return blob.Metadata;
        }

        protected void AssertIdentifierInSharedAccessPolicies(Microsoft.WindowsAzure.Storage.Blob.SharedAccessBlobPolicies sharedAccessPolicies, BlobSignedIdentifier expectedIdentifier, Microsoft.WindowsAzure.Storage.Blob.SharedAccessBlobPermissions permissions)
        {
            var policy = sharedAccessPolicies.Where(i => i.Key.Equals(expectedIdentifier.Id, StringComparison.InvariantCultureIgnoreCase)).FirstOrDefault();
            Assert.IsNotNull(policy);
            Assert.AreEqual(expectedIdentifier.AccessPolicy.StartTime, policy.Value.SharedAccessStartTime.Value.UtcDateTime);
            Assert.AreEqual(expectedIdentifier.AccessPolicy.Expiry, policy.Value.SharedAccessExpiryTime.Value.UtcDateTime);
            Assert.IsTrue(policy.Value.Permissions.HasFlag(permissions));
        }

        #endregion

        #region Setup Mechanics

        protected void CreateContainer(string containerName, Dictionary<string, string> metadata = null)
        {
            var client = StorageAccount.CreateCloudBlobClient();
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

        protected string LeaseBlob(string containerName, string blobName, TimeSpan? leaseTime = null, string leaseId = null)
        {
            var client = StorageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail("LeaseBlob: The container '{0}' does not exist", containerName);

            var blob = container.GetBlockBlobReference(blobName);

            if (!blob.Exists())
                Assert.Fail("LeaseBlob: The blob '{0}' does not exist", blobName);

            return blob.AcquireLease(leaseTime, leaseId);
        }

        protected string LeaseContainer(string containerName, TimeSpan? leaseTime, string leaseId)
        {
            var client = StorageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var lease = container.AcquireLease(leaseTime, leaseId);
            RegisterContainerForCleanup(containerName, lease);
            return lease;
        }

        protected void ReleaseContainerLease(string containerName, string lease)
        {
            var client = StorageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            container.ReleaseLease(new AccessCondition() { LeaseId = lease });
        }

        protected void BreakContainerLease(string containerName, string lease, int breakPeriod = 1)
        {
            var client = StorageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            container.BreakLease(TimeSpan.FromSeconds(breakPeriod), new AccessCondition() { LeaseId = lease });
        }

        protected void AddContainerAccessPolicy(string containerName, Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType publicAccess, string id = null, DateTime? startDate = null, DateTime? expiry = null)
        {
            var client = StorageAccount.CreateCloudBlobClient();
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

        protected Microsoft.WindowsAzure.Storage.Blob.BlobContainerPermissions GetContainerPermissions(string containerName)
        {
            var client = StorageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var permissions = container.GetPermissions();
            return permissions;
        }

        protected Microsoft.WindowsAzure.Storage.Blob.LeaseState GetContainerLeaseState(string containerName)
        {
            var client = StorageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            container.FetchAttributes();
            return container.Properties.LeaseState;
        }

        protected Microsoft.WindowsAzure.Storage.Blob.LeaseState GetBlobLeaseState(string containerName, string blobName)
        {
            var client = StorageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetBlobReferenceFromServer(blobName);
            blob.FetchAttributes();
            return blob.Properties.LeaseState;
        }

        protected List<Microsoft.WindowsAzure.Storage.Blob.ListBlockItem> CreateBlockList(string containerName, string blobName,
            IEnumerable<string> blockIdsToCreate, string dataPerBlock, Encoding encoder = null)
        {
            var client = StorageAccount.CreateCloudBlobClient();
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

        protected BlockListBlockIdList CreateBlockIdList(int idCount, BlockListListType listType)
        {
            var idList = new BlockListBlockIdList();
            for (var i = 0; i < idCount; i++)
            {
                idList.Add(new BlockListBlockId
                {
                    Id = Base64Converter.ConvertToBase64("id" + idCount),
                    ListType = listType
                });
            }
            return idList;
        }

        protected List<string> GetIdsFromBlockIdList(BlockListBlockIdList list)
        {
            return list.Select(bid => bid.Id).ToList();
        }

        protected void BreakBlobLease(string containerName, string blobName, string lease, int breakPeriod = 1)
        {
            var client = StorageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetBlobReferenceFromServer(blobName);
            blob.BreakLease(TimeSpan.FromSeconds(breakPeriod), new AccessCondition { LeaseId = lease });
        }

        protected CloudBlockBlob CreateBlockBlob(string containerName, string blobName, Dictionary<string, string> metadata = null, string content = "Generic content", string contentType = "", string contentEncoding = "", string contentLanguage = "")
        {
            var client = StorageAccount.CreateCloudBlobClient();
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

        protected void UpdateBlockBlob(string containerName, string blobName, Dictionary<string, string> metadata = null, string content = "Generic content", string contentType = "", string contentEncoding = "", string contentLanguage = "")
        {
            CreateBlockBlob(containerName, blobName, metadata, content, contentType, contentEncoding, contentLanguage);
        }

        protected void CreatePageBlob(string containerName, string blobName, Dictionary<string, string> metadata = null)
        {
            var client = StorageAccount.CreateCloudBlobClient();
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

        protected void CreateBlobUncommitted(string containerName, string blobName)
        {
            var client = StorageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);

            byte[] data = UTF8Encoding.UTF8.GetBytes("Generic content");
            // non-Base64 values fail?
            var blockId = Convert.ToBase64String(Encoding.UTF8.GetBytes("A"));
            blob.PutBlock(blockId, new MemoryStream(data), null);
        }

        protected void CopyBlob(string containerName, string sourceBlobName, string targetBlobName)
        {
            // could we have made this require more work?
            var client = StorageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var sourceBlob = container.GetBlockBlobReference(sourceBlobName);
            var targetBlob = container.GetBlockBlobReference(targetBlobName);
            targetBlob.StartCopyFromBlob(sourceBlob);
        }

        protected void SnapshotBlob(string containerName, string blobName)
        {
            var client = StorageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);
            blob.CreateSnapshot();
        }


        #endregion

        protected static DateTime GetTruncatedUtcNow()
        {
            return new DateTime(DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day, DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, DateTime.UtcNow.Second, DateTimeKind.Utc);
        }

        protected static string GetGuidString()
        {
            return Guid.NewGuid().ToString();
        }
    }
}
