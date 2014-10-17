using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.BlobService.BlobOperations;
using Basic.Azure.Storage.Communications.BlobService.ContainerOperations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.ClientContracts
{
    public interface IBlobStorageClient
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

        #endregion

        #region Blob Operations

        PutBlobResponse PutBlockBlob(string containerName, string blobName, byte[] data,
           string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
           string cacheControl = null, Dictionary<string, string> metadata = null);
        Task<PutBlobResponse> PutBlockBlobAsync(string containerName, string blobName, byte[] data,
           string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
           string cacheControl = null, Dictionary<string, string> metadata = null);

        PutBlobResponse PutPageBlob(string containerName, string blobName, int contentLength,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null, long sequenceNumber = 0);
        Task<PutBlobResponse> PutPageBlobAsync(string containerName, string blobName, int contentLength,
                    string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
                    string cacheControl = null, Dictionary<string, string> metadata = null, long sequenceNumber = 0);

        void SetContainerMetadata(string containerName, Dictionary<string, string> metadata, string lease = null);
        Task SetContainerMetadataAsync(string containerName, Dictionary<string, string> metadata, string lease = null);

        #endregion



        
    }
}
