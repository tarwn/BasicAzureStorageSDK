using Basic.Azure.Storage.Communications.ServiceExceptions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Tests.Integration
{
    [TestFixture]
    public class QueueServiceClientTests
    {
        private StorageAccountSettings _accountSettings = new LocalEmulatorAccountSettings();
        private CloudStorageAccount _storageAccount = CloudStorageAccount.Parse("UseDevelopmentStorage=true;");

        private List<string> _queuesToCleanUp = new List<string>();

        private string GenerateSampleQueueName()
        {
            var name = "unit-test-" + Guid.NewGuid().ToString().ToLower();
            _queuesToCleanUp.Add(name);
            return name;
        }

        [TestFixtureTearDown]
        public void TestFixtureTeardown()
        {
            //let's clean up!
            var client = _storageAccount.CreateCloudQueueClient();
            foreach (var queueName in _queuesToCleanUp)
            {
                var queue = client.GetQueueReference(queueName);
                queue.DeleteIfExists();
            }
        }

        [Test]
        public void CreateQueue_ValidName_CreatesQueue()
        {
            var client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            client.CreateQueue(queueName);

            AssertQueueExists(queueName);
        }

        [Test]
        public void CreateQueue_ValidNameAndMetadata_CreatesQueue()
        {
            var client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            client.CreateQueue(queueName, new Dictionary<string, string>() { 
                { "SampleName", "SampleValue" }
            });

            AssertQueueExists(queueName);
        }

        [Test]
        public void CreateQueue_AlreadyExistsWithMatchingMetadata_ReportsNoContentPerDocumentation()
        {
            var client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            client.CreateQueue(queueName);

            // I don't think I can/should-be-able-to test the return code at this level...
        }

        [Test]
        [ExpectedException(typeof(QueueAlreadyExistsAzureException))]
        public void CreateQueue_AlreadyExistsWithDifferentMetadata_ReportsConflictProperly()
        {
            var client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            client.CreateQueue(queueName, new Dictionary<string, string> { 
                { "SampleKey", "SampleValue" }
            });

            // expects exception
        }

        #region Assertions

        private void AssertQueueExists(string queueName)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            if (!queue.Exists())
            {
                Assert.Fail(String.Format("The queue '{0}' does not exist", queueName));
            }
        }

        #endregion

        #region Setup Mechanics

        private void CreateQueue(string queueName)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            queue.Create();
        }

        #endregion
    }
}
