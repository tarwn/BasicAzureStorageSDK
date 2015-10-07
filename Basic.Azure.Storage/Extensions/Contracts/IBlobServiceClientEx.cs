using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.BlobService.BlobOperations;

namespace Basic.Azure.Storage.Extensions.Contracts
{
    public interface IBlobServiceClientEx : IBlobServiceClient
    {
        IBlobOrBlockListResponseWrapper PutBlockBlobIntelligently(int blockSize,
            string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null);
        IBlobOrBlockListResponseWrapper PutBlockBlobIntelligently(int blockSize,
            string containerName, string blobName, byte[] data, string leaseId,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null);
        Task<IBlobOrBlockListResponseWrapper> PutBlockBlobIntelligentlyAsync(int blockSize,
            string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null);
        Task<IBlobOrBlockListResponseWrapper> PutBlockBlobIntelligentlyAsync(int blockSize,
            string containerName, string blobName, Stream data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null);
        Task<IBlobOrBlockListResponseWrapper> PutBlockBlobIntelligentlyAsync(int blockSize,
            string containerName, string blobName, byte[] data, string leaseId,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null);
        Task<IBlobOrBlockListResponseWrapper> PutBlockBlobIntelligentlyAsync(int blockSize,
            string containerName, string blobName, Stream data, string leaseId,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null);

        PutBlockListResponse PutBlockBlobAsList(int blockSize,
            string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null);
        PutBlockListResponse PutBlockBlobAsList(int blockSize,
            string containerName, string blobName, byte[] data, string leaseId,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null);
        Task<PutBlockListResponse> PutBlockBlobAsListAsync(int blockSize,
            string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null);
        Task<PutBlockListResponse> PutBlockBlobAsListAsync(int blockSize,
            string containerName, string blobName, Stream data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null);
        Task<PutBlockListResponse> PutBlockBlobAsListAsync(int blockSize,
            string containerName, string blobName, byte[] data, string leaseId,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null);
        Task<PutBlockListResponse> PutBlockBlobAsListAsync(int blockSize,
            string containerName, string blobName, Stream data, string leaseId,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null);
    }
}