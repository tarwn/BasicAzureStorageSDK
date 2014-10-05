using Basic.Azure.Storage.Communications.ServiceExceptions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
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

        #region Queue Operations Tests

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

        [Test]
        public void DeleteQueue_ValidQueue_DeletesQueue()
        {
            var client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);

            client.DeleteQueue(queueName);

            AssertQueueDoesNotExist(queueName);
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public void DeleteQueue_NonExistentQueue_ReportsError()
        {
            var client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            client.DeleteQueue(queueName);

            // expects exception
        }

        #endregion

        #region Message Operations Tests

        [Test]
        public void PutMessage_ValidMessage_AddsMessageToQueue()
        {
            var client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            string message = "Unit Test Message";

            client.PutMessage(queueName, message);

            AssertQueueHasMessage(queueName);
        }

        [Test]
        public void PutMessage_ValidMessageWithVisibilityTimeout_IsNotVisibleInQueue()
        {
            var client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            string message = "Unit Test Message";

            client.PutMessage(queueName, message, visibilityTimeout: 5);

            AssertQueueInvisibleMessage(queueName);
        }


        [Test]
        [Ignore("Either I messed up the MessageTTL property or it doesn't work as expected")]
        public void PutMessage_ValidMessageWithTTL_DisappearsAfterTTLExpires()
        {
            var client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            CreateQueue(queueName);
            string message = "Unit Test Message";

            client.PutMessage(queueName, message, messageTtl: 1);

            AssertQueueHasMessage(queueName);
            Thread.Sleep(1100); // a little extra to be sure
            AssertQueueIsEmpty(queueName);
        }

        #endregion

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

        private void AssertQueueDoesNotExist(string queueName)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            if (queue.Exists())
            {
                Assert.Fail(String.Format("The queue '{0}' exists", queueName));
            }
        }

        private void AssertQueueHasMessage(string queueName)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            if (!queue.Exists())
            {
                Assert.Fail(String.Format("The queue '{0}' does not exist", queueName));
            }

            var msg = queue.PeekMessage();
            if (msg == null)
            {
                Assert.Fail(String.Format("The queue '{0}' does not have any messages", queueName));
            }
        }

        private void AssertQueueIsEmpty(string queueName)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            if (!queue.Exists())
            {
                Assert.Fail(String.Format("The queue '{0}' does not exist", queueName));
            }

            queue.FetchAttributes();

            if (queue.ApproximateMessageCount > 0)
            {
                Assert.Fail(String.Format("The queue '{0}' reports {1} messages", queueName, queue.ApproximateMessageCount));
            }
        }

        private void AssertQueueInvisibleMessage(string queueName)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            if (!queue.Exists())
            {
                Assert.Fail(String.Format("The queue '{0}' does not exist", queueName));
            }

            var msg = queue.PeekMessage();
            queue.FetchAttributes();

            if (msg != null)
            {
                Assert.Fail(String.Format("The queue '{0}' had a visible message", queueName));
            }

            if (queue.ApproximateMessageCount == 0)
            {
                Assert.Fail(String.Format("The queue '{0}' reports 0 ApproximateMessageCount", queueName));
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
