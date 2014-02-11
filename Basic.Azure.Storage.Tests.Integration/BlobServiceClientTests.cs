using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Tests.Integration
{
    [TestFixture]
    public class BlobServiceClientTests
    {

        private StorageAccountSettings _accountSettings = new LocalEmulatorAccountSettings();
        private CloudStorageAccount _storageAccount = CloudStorageAccount.Parse("UseDevelopmentStorage=true;");

        private List<string> _containersToCleanUp = new List<string>();

        private string GenerateSampleContainerName()
        {
            var name = "unit-test-" + Guid.NewGuid().ToString().ToLower();
            _containersToCleanUp.Add(name);
            return name;
        }

        [TestFixtureTearDown]
        public void TestFixtureTeardown()
        {
            //let's clean up!
            var client = _storageAccount.CreateCloudBlobClient();
            foreach (var containerName in _containersToCleanUp)
            {
                var container = client.GetContainerReference(containerName);
                container.DeleteIfExists();
            }
        }

        #region Container Operations Tests

        [Test]
        public void CreateContainer_ValidArguments_CreatesContainerWithSpecificName()
        {
            var containerName = GenerateSampleContainerName();
            var client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.None);

            AssertContainerExists(containerName);
        }

        [Test]
        public void CreateContainer_ValidArgumentsWithPublicContainerAccess_CreatesContainerWithContainerAccess()
        {
            var containerName = GenerateSampleContainerName();
            var client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.PublicContainer);

            AssertContainerAccess(containerName, BlobContainerPublicAccessType.Container);
        }

        [Test]
        public void CreateContainer_ValidArgumentsWithPublicBlobAccess_CreatesContainerWithBlobAccess()
        {
            var containerName = GenerateSampleContainerName();
            var client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.PublicBlob);

            AssertContainerAccess(containerName, BlobContainerPublicAccessType.Blob);
        }

        [Test]
        public void CreateContainer_ValidArgumentsWithNoPublicAccess_CreatesContainerWithNoPublicAccess()
        {
            var containerName = GenerateSampleContainerName();
            var client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.None);

            AssertContainerAccess(containerName, BlobContainerPublicAccessType.Off);
        }

        [Test]
        [ExpectedException(typeof(ContainerAlreadyExistsAzureException))]
        public void CreateContainer_AlreadyExists_ThrowsContainerAlreadyExistsException()
        {
            var containerName = GenerateSampleContainerName();
            CreateContainer(containerName);
            var client = new BlobServiceClient(_accountSettings);

            client.CreateContainer(containerName, ContainerAccessType.None);

            // expects exception
        }

        [Test]
        public void CreateContainer_AlreadyExists_ReturnsContainerCreationResponse()
        {
            var containerName = GenerateSampleContainerName();
            var client = new BlobServiceClient(_accountSettings);

            var response = client.CreateContainer(containerName, ContainerAccessType.None);

            Assert.IsTrue(response.Date > DateTime.Now.AddSeconds(-5), String.Format("Response Date was set to {0}", response.Date));
            Assert.IsTrue(response.LastModified > DateTime.Now.AddSeconds(-5), String.Format("Response LastModified was set to {0}", response.LastModified));
            Assert.IsFalse(string.IsNullOrWhiteSpace(response.ETag), "Response ETag is not set");

        }

        #endregion

        #region Assertions

        private void AssertContainerExists(string containerName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail(String.Format("The container '{0}' does not exist", containerName));
        }

        private void AssertContainerAccess(string containerName, BlobContainerPublicAccessType containerAccessType)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (!container.Exists())
                Assert.Fail(String.Format("The container '{0}' does not exist", containerName));

            var permissions = container.GetPermissions();
            Assert.AreEqual(containerAccessType, permissions.PublicAccess, String.Format("Container access was expected to be {0}, but it is actually {1}", containerAccessType, permissions.PublicAccess));
        }

        #endregion

        #region Setup Mechanics

        private void CreateContainer(string containerName)
        {
            var client = _storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            container.Create();
        }

        #endregion

    }
}
