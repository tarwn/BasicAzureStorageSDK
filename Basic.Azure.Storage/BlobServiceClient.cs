using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.BlobService.BlobOperations;
using Basic.Azure.Storage.Communications.BlobService.ContainerOperations;
using Basic.Azure.Storage.Communications.Common;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Basic.Azure.Storage
{
    public class BlobServiceClient : IBlobServiceClient
    {
        private StorageAccountSettings _account;

        public BlobServiceClient(StorageAccountSettings account)
        {
            _account = account;
        }

        #region Account Operations


        #endregion

        #region Container Operations

        public CreateContainerResponse CreateContainer(string containerName, ContainerAccessType containerAccessType)
        {
            var request = new CreateContainerRequest(_account, containerName, containerAccessType);
            var response = request.Execute();
            return response.Payload;
        }
        public async Task<CreateContainerResponse> CreateContainerAsync(string containerName, ContainerAccessType containerAccessType)
        {
            var request = new CreateContainerRequest(_account, containerName, containerAccessType);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }

        public GetContainerPropertiesResponse GetContainerProperties(string containerName)
        {
            var request = new GetContainerPropertiesRequest(_account, containerName);
            var response = request.Execute();
            return response.Payload;
        }
        public async Task<GetContainerPropertiesResponse> GetContainerPropertiesAsync(string containerName)
        {
            var request = new GetContainerPropertiesRequest(_account, containerName);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }

        public GetContainerMetadataResponse GetContainerMetadata(string containerName)
        {
            var request = new GetContainerMetadataRequest(_account, containerName);
            var response = request.Execute();
            return response.Payload;
        }
        public async Task<GetContainerMetadataResponse> GetContainerMetadataAsync(string containerName)
        {
            var request = new GetContainerMetadataRequest(_account, containerName);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }

        public void SetContainerMetadata(string containerName, Dictionary<string, string> metadata, string lease = null)
        {
            var request = new SetContainerMetadataRequest(_account, containerName, metadata, lease);
            request.Execute();
        }
        public async Task SetContainerMetadataAsync(string containerName, Dictionary<string, string> metadata, string lease = null)
        {
            var request = new SetContainerMetadataRequest(_account, containerName, metadata, lease);
            await request.ExecuteAsync();
        }

        public GetContainerACLResponse GetContainerACL(string containerName)
        {
            var request = new GetContainerACLRequest(_account, containerName);
            var response = request.Execute();
            return response.Payload;
        }
        public async Task<GetContainerACLResponse> GetContainerACLAsync(string containerName)
        {
            var request = new GetContainerACLRequest(_account, containerName);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }

        public void SetContainerACL(string containerName, ContainerAccessType containerAccess, List<BlobSignedIdentifier> signedIdentifiers, string leaseId = null)
        {
            var request = new SetContainerACLRequest(_account, containerName, containerAccess, signedIdentifiers, leaseId);
            request.Execute();
        }
        public async Task SetContainerACLAsync(string containerName, ContainerAccessType containerAccess, List<BlobSignedIdentifier> signedIdentifiers, string leaseId = null)
        {
            var request = new SetContainerACLRequest(_account, containerName, containerAccess, signedIdentifiers, leaseId);
            await request.ExecuteAsync();
        }

        public void DeleteContainer(string containerName, string leaseId = null)
        {
            var request = new DeleteContainerRequest(_account, containerName, leaseId);
            request.Execute();
        }
        public async Task DeleteContainerAsync(string containerName, string leaseId = null)
        {
            var request = new DeleteContainerRequest(_account, containerName, leaseId);
            await request.ExecuteAsync();
        }

        public LeaseContainerAcquireResponse LeaseContainerAcquire(string containerName, int leaseDurationInSeconds = -1, string proposedLeaseId = null)
        {
            var request = new LeaseContainerAcquireRequest(_account, containerName, leaseDurationInSeconds, proposedLeaseId);
            var response = request.Execute();
            return response.Payload;
        }
        public async Task<LeaseContainerAcquireResponse> LeaseContainerAcquireAsync(string containerName, int leaseDurationInSeconds = -1, string proposedLeaseId = null)
        {
            var request = new LeaseContainerAcquireRequest(_account, containerName, leaseDurationInSeconds, proposedLeaseId);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }

        public LeaseContainerRenewResponse LeaseContainerRenew(string containerName, string leaseId)
        {
            var request = new LeaseContainerRenewRequest(_account, containerName, leaseId);
            var response = request.Execute();
            return response.Payload;
        }
        public async Task<LeaseContainerRenewResponse> LeaseContainerRenewAsync(string containerName, string leaseId)
        {
            var request = new LeaseContainerRenewRequest(_account, containerName, leaseId);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }
        public LeaseContainerChangeResponse LeaseContainerChange(string containerName, string currentLeaseid, string proposedLeaseId)
        {
            var request = new LeaseContainerChangeRequest(_account, containerName, currentLeaseid, proposedLeaseId);
            var response = request.Execute();
            return response.Payload;
        }
        public async Task<LeaseContainerChangeResponse> LeaseContainerChangeAsync(string containerName, string currentLeaseid, string proposedLeaseId)
        {
            var request = new LeaseContainerChangeRequest(_account, containerName, currentLeaseid, proposedLeaseId);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }
        public void LeaseContainerRelease(string containerName, string leaseId)
        {
            var request = new LeaseContainerReleaseRequest(_account, containerName, leaseId);
            request.Execute();
        }
        public async Task LeaseContainerReleaseAsync(string containerName, string leaseId)
        {
            var request = new LeaseContainerReleaseRequest(_account, containerName, leaseId);
            await request.ExecuteAsync();
        }
        public void LeaseContainerBreak(string containerName, string leaseId, int leaseBreakPeriod)
        {
            var request = new LeaseContainerBreakRequest(_account, containerName, leaseId, leaseBreakPeriod);
            request.Execute();
        }
        public async Task LeaseContainerBreakAsync(string containerName, string leaseId, int leaseBreakPeriod)
        {
            var request = new LeaseContainerBreakRequest(_account, containerName, leaseId, leaseBreakPeriod);
            await request.ExecuteAsync();
        }

        public ListBlobsResponse ListBlobs(string containerName, string prefix = "", string delimiter = "", string marker = "", int maxResults = 5000, ListBlobsInclude? include = null)
        {
            var request = new ListBlobsRequest(_account, containerName, prefix, delimiter, marker, maxResults, include);
            var response = request.Execute();
            return response.Payload;
        }
        public async Task<ListBlobsResponse> ListBlobsAsync(string containerName, string prefix = "", string delimiter = "", string marker = "", int maxResults = 5000, ListBlobsInclude? include = null)
        {
            var request = new ListBlobsRequest(_account, containerName, prefix, delimiter, marker, maxResults, include);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }

        #endregion

        #region Blob Operations

        /// <summary>
        /// Creates a new BlockBlob (Alias for the PutBlob call with a Blob Type of BlockBlob)
        /// </summary>
        public PutBlobResponse PutBlockBlob(string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null, string leaseId = null)
        {
            var request = new PutBlobRequest(_account, containerName, blobName, data, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata, leaseId);
            var response = request.Execute();
            return response.Payload;
        }
        public async Task<PutBlobResponse> PutBlockBlobAsync(string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null, string leaseId = null)
        {
            var request = new PutBlobRequest(_account, containerName, blobName, data, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata, leaseId);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }

        public PutBlockListResponse PutBlockList(string containerName, string blobName, BlockListBlockIdList data,
            string cacheControl = null, string contentType = null,
            string contentEncoding = null, string contentLanguage = null, string blobContentMD5 = null,
            Dictionary<string, string> metadata = null, string leaseId = null)
        {
            var request = new PutBlockListRequest(_account, containerName, blobName, data, cacheControl, contentType, contentEncoding, contentLanguage, blobContentMD5, metadata, leaseId);
            var response = request.Execute();
            return response.Payload;
        }
        public async Task<PutBlockListResponse> PutBlockListAsync(string containerName, string blobName, BlockListBlockIdList data,
            string cacheControl = null, string contentType = null,
            string contentEncoding = null, string contentLanguage = null, string blobContentMD5 = null,
            Dictionary<string, string> metadata = null, string leaseId = null)
        {
            var request = new PutBlockListRequest(_account, containerName, blobName, data, cacheControl, contentType, contentEncoding, contentLanguage, blobContentMD5, metadata, leaseId);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }

        public PutBlockResponse PutBlock(string containerName, string blobName, string blockId, byte[] data, string contentMD5 = null, string leaseId = null)
        {
            var request = new PutBlockRequest(_account, containerName, blobName, blockId, data, contentMD5, leaseId);
            var response = request.Execute();
            return response.Payload;
        }
        public async Task<PutBlockResponse> PutBlockAsync(string containerName, string blobName, string blockId, byte[] data, string contentMD5 = null, string leaseId = null)
        {
            var request = new PutBlockRequest(_account, containerName, blobName, blockId, data, contentMD5, leaseId);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }

        /// <summary>
        /// Creates a new PageBlob (Alias for the PutBlob call with a Blob Type of PageBlob)
        /// </summary>
        public PutBlobResponse PutPageBlob(string containerName, string blobName, int contentLength,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null, long sequenceNumber = 0, string leaseId = null)
        {
            var request = new PutBlobRequest(_account, containerName, blobName, contentLength, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata, sequenceNumber, leaseId);
            var response = request.Execute();
            return response.Payload;
        }
        public async Task<PutBlobResponse> PutPageBlobAsync(string containerName, string blobName, int contentLength,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null, long sequenceNumber = 0, string leaseId = null)
        {
            var request = new PutBlobRequest(_account, containerName, blobName, contentLength, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata, sequenceNumber, leaseId);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }

        public void DeleteBlob(string containerName, string blobName, string leaseId = null)
        {
            var request = new DeleteBlobRequest(_account, containerName, blobName, leaseId);
            request.Execute();
        }

        public async Task DeleteBlobAsync(string containerName, string blobName, string leaseId = null)
        {
            var request = new DeleteBlobRequest(_account, containerName, blobName, leaseId);
            await request.ExecuteAsync();
        }

        public GetBlobResponse GetBlob(string containerName, string blobName, BlobRange range = null, string leaseId = null)
        {
            var request = new GetBlobRequest(_account, containerName, blobName, range, leaseId);
            var response = request.Execute();
            return response.Payload;
        }

        public async Task<GetBlobResponse> GetBlobAsync(string containerName, string blobName, BlobRange range = null, string leaseId = null)
        {
            var request = new GetBlobRequest(_account, containerName, blobName, range, leaseId);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }

        public LeaseBlobAcquireResponse LeaseBlobAcquire(string containerName, string blobName, int leaseDurationInSeconds = -1, string proposedLeaseId = null)
        {
            var request = new LeaseBlobAcquireRequest(_account, containerName, blobName, leaseDurationInSeconds, proposedLeaseId);
            var response = request.Execute();
            return response.Payload;
        }

        public async Task<LeaseBlobAcquireResponse> LeaseBlobAcquireAsync(string containerName, string blobName, int leaseDurationInSeconds = -1, string proposedLeaseId = null)
        {
            var request = new LeaseBlobAcquireRequest(_account, containerName, blobName, leaseDurationInSeconds, proposedLeaseId);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }

        public LeaseBlobRenewResponse LeaseBlobRenew(string containerName, string blobName, string leaseId)
        {
            var request = new LeaseBlobRenewRequest(_account, containerName, blobName, leaseId);
            var response = request.Execute();
            return response.Payload;
        }
        public async Task<LeaseBlobRenewResponse> LeaseBlobRenewAsync(string containerName, string blobName, string leaseId)
        {
            var request = new LeaseBlobRenewRequest(_account, containerName, blobName, leaseId);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }

        public LeaseBlobChangeResponse LeaseBlobChange(string containerName, string blobName, string currentLeaseId, string proposedLeaseId)
        {
            var request = new LeaseBlobChangeRequest(_account, containerName, blobName, currentLeaseId, proposedLeaseId);
            var response = request.Execute();
            return response.Payload;
        }

        public async Task<LeaseBlobChangeResponse> LeaseBlobChangeAsync(string containerName, string blobName, string currentLeaseId, string proposedLeaseId)
        {
            var request = new LeaseBlobChangeRequest(_account, containerName, blobName, currentLeaseId, proposedLeaseId);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }

        #endregion

    }
}
