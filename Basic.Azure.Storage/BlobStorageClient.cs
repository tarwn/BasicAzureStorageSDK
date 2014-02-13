using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.BlobService.BlobOperations;
using Basic.Azure.Storage.Communications.BlobService.ContainerOperations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage
{
    public class BlobServiceClient
    {
        private StorageAccountSettings _account;

        public BlobServiceClient(StorageAccountSettings account)
        {
            _account = account;
        }

        public CreateContainerResponse CreateContainer(string containerName, ContainerAccessType containerAccessType)
        {
            var request = new CreateContainerRequest(_account, containerName, containerAccessType);
            var response = request.Execute();
            return response.Payload;
        }

        /// <summary>
        /// Creates a new BlockBlob (Alias for the PutBlob call with a Blob Type of BlockBlob)
        /// </summary>
        public PutBlobResponse PutBlockBlob(string containerName, string blobName, byte[] data, 
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string,string> metadata = null)
        {
            var request = new PutBlobRequest(_account, containerName, blobName, data, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata);
            var response = request.Execute();
            return response.Payload;
        }

        /// <summary>
        /// Creates a new PageBlob (Alias for the PutBlob call with a Blob Type of PageBlob)
        /// </summary>
        public PutBlobResponse PutPageBlob(string containerName, string blobName, int contentLength,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null, long sequenceNumber = 0)
        {
            var request = new PutBlobRequest(_account, containerName, blobName, contentLength, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata, sequenceNumber);
            var response = request.Execute();
            return response.Payload;
        }
    }
}
