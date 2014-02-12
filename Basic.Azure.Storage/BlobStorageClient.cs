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

        public void PutBlob(string containerName, string blobName, BlobType blobType, byte[] data, 
            string contentType = null, string contentEncoding = null, string contentLanguage = null)
        {
            var request = new PutBlobRequest(_account, containerName, blobName, blobType, data, contentType, contentEncoding, contentLanguage);
            var response = request.Execute();
        }
    }
}
