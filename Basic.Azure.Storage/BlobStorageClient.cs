using Basic.Azure.Storage.Communications.BlobService;
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

        public void CreateContainer(string containerName, ContainerAccessType containerAccessType)
        {
            var request = new CreateContainerRequest(_account, containerName, containerAccessType);
            request.Execute();
        }
    }
}
