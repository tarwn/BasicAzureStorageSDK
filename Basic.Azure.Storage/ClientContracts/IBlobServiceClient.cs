﻿using System.Collections.Generic;
using System.Threading.Tasks;
using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.BlobService.BlobOperations;
using Basic.Azure.Storage.Communications.BlobService.ContainerOperations;
using Basic.Azure.Storage.Communications.Common;

namespace Basic.Azure.Storage.ClientContracts
{
    public interface IBlobServiceClient
    {
        #region Account Operations

        #endregion

        #region Container Operations

        CreateContainerResponse CreateContainer(string containerName, ContainerAccessType containerAccessType);
        Task<CreateContainerResponse> CreateContainerAsync(string containerName, ContainerAccessType containerAccessType);

        GetContainerPropertiesResponse GetContainerProperties(string containerName);
        Task<GetContainerPropertiesResponse> GetContainerPropertiesAsync(string containerName);

        GetContainerMetadataResponse GetContainerMetadata(string containerName);
        Task<GetContainerMetadataResponse> GetContainerMetadataAsync(string containerName);

        void SetContainerMetadata(string containerName, Dictionary<string, string> metadata, string lease = null);
        Task SetContainerMetadataAsync(string containerName, Dictionary<string, string> metadata, string lease = null);

        GetContainerACLResponse GetContainerACL(string containerName);
        Task<GetContainerACLResponse> GetContainerACLAsync(string containerName);

        void SetContainerACL(string containerName, ContainerAccessType containerAccess, List<BlobSignedIdentifier> signedIdentifiers, string leaseId = null);
        Task SetContainerACLAsync(string containerName, ContainerAccessType containerAccess, List<BlobSignedIdentifier> signedIdentifiers, string leaseId = null);

        void DeleteContainer(string containerName, string leaseId = null);
        Task DeleteContainerAsync(string containerName, string leaseId = null);

        LeaseContainerAcquireResponse LeaseContainerAcquire(string containerName, int leaseDurationInSeconds = -1, string proposedLeaseId = null);
        Task<LeaseContainerAcquireResponse> LeaseContainerAcquireAsync(string containerName, int leaseDurationInSeconds = -1, string proposedLeaseId = null);
        LeaseContainerRenewResponse LeaseContainerRenew(string containerName, string leaseId);
        Task<LeaseContainerRenewResponse> LeaseContainerRenewAsync(string containerName, string leaseId);
        LeaseContainerChangeResponse LeaseContainerChange(string containerName, string currentLeaseId, string proposedLeaseId);
        Task<LeaseContainerChangeResponse> LeaseContainerChangeAsync(string containerName, string currentLeaseId, string proposedLeaseId);
        void LeaseContainerRelease(string containerName, string leaseId);
        Task LeaseContainerReleaseAsync(string containerName, string leaseId);
        void LeaseContainerBreak(string containerName, string leaseId, int leaseBreakPeriod);
        Task LeaseContainerBreakAsync(string containerName, string leaseId, int leaseBreakPeriod);

        ListBlobsResponse ListBlobs(string containerName, string prefix = "", string delimiter = "", string marker = "", int maxResults = 5000, ListBlobsInclude? include = null);
        Task<ListBlobsResponse> ListBlobsAsync(string containerName, string prefix = "", string delimiter = "", string marker = "", int maxResults = 5000, ListBlobsInclude? include = null);

        #endregion

        #region Blob Operations

        /// <summary>
        /// Creates a new BlockBlob (Alias for the PutBlob call with a Blob Type of BlockBlob)
        /// </summary>
        PutBlobResponse PutBlockBlob(string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null, string leaseId = null);
        Task<PutBlobResponse> PutBlockBlobAsync(string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null, string leaseId = null);

        /// <summary>
        /// Creates a new PageBlob (Alias for the PutBlob call with a Blob Type of PageBlob)
        /// </summary>
        PutBlobResponse PutPageBlob(string containerName, string blobName, int contentLength,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null, long sequenceNumber = 0, string leaseId = null);
        Task<PutBlobResponse> PutPageBlobAsync(string containerName, string blobName, int contentLength,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null, long sequenceNumber = 0, string leaseId = null);

        GetBlobResponse GetBlob(string containerName, string blobName, BlobRange range = null, string leaseId = null);
        Task<GetBlobResponse> GetBlobAsync(string containerName, string blobName, BlobRange range = null, string leaseId = null);


        GetBlobPropertiesResponse GetBlobProperties(string containerName, string blobName, string leaseId = null);
        Task<GetBlobPropertiesResponse> GetBlobPropertiesAsync(string containerName, string blobName, string leaseId = null);

        GetBlobMetadataResponse GetBlobMetadata(string containerName, string blobName, string leaseId = null);
        Task<GetBlobMetadataResponse> GetBlobMetadataAsync(string containerName, string blobName, string leaseId = null);

        SetBlobMetadataResponse SetBlobMetadata(string containerName, string blobName, Dictionary<string, string> metadata, string leaseId = null);
        Task<SetBlobMetadataResponse> SetBlobMetadataAsync(string containerName, string blobName, Dictionary<string, string> metadata, string leaseId = null);

        LeaseBlobAcquireResponse LeaseBlobAcquire(string containerName, string blobName, int leaseDurationInSeconds = 60, string proposedLeaseId = null);
        Task<LeaseBlobAcquireResponse> LeaseBlobAcquireAsync(string containerName, string blobName, int leaseDurationInSeconds = 60, string proposedLeaseId = null);

        LeaseBlobRenewResponse LeaseBlobRenew(string containerName, string blobName, string leaseId);
        Task<LeaseBlobRenewResponse> LeaseBlobRenewAsync(string containerName, string blobName, string leaseId);

        LeaseBlobChangeResponse LeaseBlobChange(string containerName, string blobName, string currentLeaseId, string proposedLeaseId);
        Task<LeaseBlobChangeResponse> LeaseBlobChangeAsync(string containerName, string blobName, string currentLeaseId, string proposedLeaseId);

        void LeaseBlobRelease(string containerName, string blobName, string leaseId);
        Task LeaseBlobReleaseAsync(string containerName, string blobName, string leaseId);

        void LeaseBlobBreak(string containerName, string blobName, string leaseId, int leaseBreakPeriod);
        Task LeaseBlobBreakAsync(string containerName, string blobName, string leaseId, int leaseBreakPeriod);

        CopyBlobResponse CopyBlob(string containerName, string blobName, string copySource, Dictionary<string, string> metadata = null, string leaseId = null);
        Task<CopyBlobResponse> CopyBlobAsync(string containerName, string blobName, string copySource, Dictionary<string, string> metadata = null, string leaseId = null);

        void DeleteBlob(string containerName, string blobName, string leaseId = null);
        Task DeleteBlobAsync(string containerName, string blobName, string leaseId = null);

        #region Block Blob Operations

        PutBlockResponse PutBlock(string containerName, string blobName, string blockId, byte[] data, string contentMD5 = null, string leaseId = null);
        Task<PutBlockResponse> PutBlockAsync(string containerName, string blobName, string blockId, byte[] data, string contentMD5 = null, string leaseId = null);

        PutBlockListResponse PutBlockList(string containerName, string blobName, BlockListBlockIdList data,
            string cacheControl = null, string contentType = null,
            string contentEncoding = null, string contentLanguage = null, string blobContentMD5 = null,
            Dictionary<string, string> metadata = null, string leaseId = null);
        Task<PutBlockListResponse> PutBlockListAsync(string containerName, string blobName, BlockListBlockIdList data,
            string cacheControl = null, string contentType = null,
            string contentEncoding = null, string contentLanguage = null, string blobContentMD5 = null,
            Dictionary<string, string> metadata = null, string leaseId = null);

        GetBlockListResponse GetBlockList(string containerName, string blobName, string leaseId = null, GetBlockListListType blockListType = GetBlockListListType.Committed);
        Task<GetBlockListResponse> GetBlockListAsync(string containerName, string blobName, string leaseId = null, GetBlockListListType blockListType = GetBlockListListType.Committed);

        #endregion

        #region Page Blob Operations

        #endregion

        #region Append Blobs

        #endregion

        #endregion
    }
}
