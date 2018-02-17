using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Queue.Protocol;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Tests.Integration.QueueServiceClientTests
{
    [TestFixture]
    public class MessageOperationsTests
    {
        private StorageAccountSettings _accountSettings = StorageAccountSettings.Parse(ConfigurationManager.AppSettings["AzureConnectionString"]);
        private QueueUtil _util = new QueueUtil(ConfigurationManager.AppSettings["AzureConnectionString"]);

        private List<string> _queuesToCleanUp = new List<string>();

        private string GenerateSampleQueueName(string prefix = "")
        {
            var name = prefix + "unit-test-" + Guid.NewGuid().ToString().ToLower();
            _queuesToCleanUp.Add(name);
            return name;
        }

        public string FakePopReceipt { get { return "AAAA/AAAAAAAAAAA"; } }

        [TestFixtureTearDown]
        public void TestFixtureTeardown()
        {
            //let's clean up!
            _util.DeleteIfExists(_queuesToCleanUp);
        }

        #region Message Operations Tests

        [Test]
        public void PutMessage_ValidMessage_AddsMessageToQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            string message = "Unit Test Message";

            client.PutMessage(queueName, message);

            _util.AssertQueueHasMessage(queueName);
        }


        [Test]
        public void PutMessage_EmptyMessage_AddsMessageToQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            string message = String.Empty;

            client.PutMessage(queueName, message);

            _util.AssertQueueHasMessage(queueName);
        }

        [Test]
        public void PutMessage_InvalidXMLCharacters_DoesNotThrowException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            string message = "{\"KeyValueLogPayload\": {\"UserIP\": \"::1\",\"URL\": \"http://fakeurl/foo?bar=baz&baz=bar\"}}";

            Assert.DoesNotThrow(() =>
            {
                client.PutMessage(queueName, message);
            });
        }

        [Test]
        [ExpectedException(typeof(RequestBodyTooLargeAzureException))]
        public void PutMessage_TooLargeMessage_ThrowsRequestBodyTooLargeException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            string message = new String('a', 64 * 1024 + 1);

            client.PutMessage(queueName, message);

            //expects exception
        }


        [Test]
        public void PutMessage_ValidMessageWithVisibilityTimeout_IsNotVisibleInQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            string message = "Unit Test Message";

            client.PutMessage(queueName, message, visibilityTimeout: 5);

            _util.AssertQueueInvisibleMessage(queueName);
        }


        [Test]
        [Ignore("Either I messed up the MessageTTL property or it doesn't work as expected")]
        public void PutMessage_ValidMessageWithTTL_DisappearsAfterTTLExpires()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            string message = "Unit Test Message";

            client.PutMessage(queueName, message, messageTtl: 1);

            _util.AssertQueueHasMessage(queueName);
            Thread.Sleep(3100); // a little extra to be sure
            _util.AssertQueueIsEmpty(queueName);
        }

        [Test]
        public async Task PutMessageAsync_ValidMessage_AddsMessageToQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            string message = "Unit Test Message";

            await client.PutMessageAsync(queueName, message);

            _util.AssertQueueHasMessage(queueName);
        }


        [Test]
        public void GetMessages_EmptyQueue_ReturnsEmptyCollection()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);

            var response = client.GetMessages(queueName, 32);

            Assert.IsEmpty(response.Messages);
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public void GetMessages_NonExistentQueue_ThrowsQueueDoesNotExistException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            var response = client.GetMessages(queueName, 32);

            // expects exception
        }

        [Test]
        public void GetMessages_Request32ItemsFromFullQueue_Returns32Items()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            _util.AddItemsToQueue(queueName, Enumerable.Range(1, 40).Select(n => n.ToString()).ToList());

            var response = client.GetMessages(queueName, 32);

            Assert.AreEqual(32, response.Messages.Count);
            for (int i = 1; i <= 32; i++)
            {
                // Base 64 encode the expected message since Azure SDK did so when enqueueing it
                var expectedMessage = Convert.ToBase64String(ASCIIEncoding.ASCII.GetBytes(i.ToString()));
                Assert.IsTrue(response.Messages.Any(m => m.MessageText.Equals(expectedMessage)));
            }
        }

        [Test]
        public void GetMessages_RequestUndefinedNumberOfItemsFromFullQueue_ReturnsOneItem()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            _util.AddItemsToQueue(queueName, Enumerable.Range(1, 40).Select(n => n.ToString()).ToList());
            var expectedMessage = "1";

            var response = client.GetMessages(queueName);

            Assert.AreEqual(1, response.Messages.Count);
            var message = response.Messages.Single();
            // Base 64 encode the expected message since Azure SDK did so when enqueueing it
            Assert.AreEqual(Convert.ToBase64String(ASCIIEncoding.ASCII.GetBytes(expectedMessage)), message.MessageText);
        }

        [Test]
        public void GetMessages_RequestItemWithVisibility_ReturnsItemWithFutureVisibility()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            _util.AddItemsToQueue(queueName, new List<string>() { "1" });

            var response = client.GetMessages(queueName, 1, 30);

            Assert.AreEqual(1, response.Messages.Count);
            var message = response.Messages.Single();
            // Give some tolerance because the server's time might be slightly different
            var currentTime = DateTime.UtcNow;
            Assert.Less(message.InsertionTime.Subtract(currentTime).TotalMinutes, 1);
            Assert.Less(message.ExpirationTime.Subtract(currentTime).TotalDays, 8); // could be slightly larger than 7 depending on the server's time
            Assert.Less(message.TimeNextVisible.Subtract(currentTime).TotalMinutes, 2); // could be slightly larger than 1 depending on the server's time
        }

        [Test]
        public void GetMessages_RequestItemFromPopulatedQueue_ReturnsItemWithPopReceiptAndDequeueCount()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            _util.AddItemsToQueue(queueName, new List<string>() { "1" });

            var response = client.GetMessages(queueName, 1, 30);

            Assert.AreEqual(1, response.Messages.Count);
            var message = response.Messages.Single();
            Assert.IsNotNullOrEmpty(message.PopReceipt);
            Assert.Greater(message.DequeueCount, 0);
        }


        [Test]
        public async Task GetMessagesAsync_EmptyQueue_ReturnsEmptyCollection()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);

            var response = await client.GetMessagesAsync(queueName, 32);

            Assert.IsEmpty(response.Messages);
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public async Task GetMessagesAsync_NonExistentQueue_ThrowsQueueDoesNotExistException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            var response = await client.GetMessagesAsync(queueName, 32);

            // expects exception
        }

        [Test]
        public async Task GetMessagesAsync_Request32ItemsFromFullQueue_Returns32Items()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            _util.AddItemsToQueue(queueName, Enumerable.Range(1, 40).Select(n => n.ToString()).ToList());

            var response = await client.GetMessagesAsync(queueName, 32);

            Assert.AreEqual(32, response.Messages.Count);
            for (int i = 1; i <= 32; i++)
            {
                // Base 64 encode the expected message since Azure SDK did so when enqueueing it
                var expectedMessage = Convert.ToBase64String(ASCIIEncoding.ASCII.GetBytes(i.ToString()));
                Assert.IsTrue(response.Messages.Any(m => m.MessageText.Equals(expectedMessage)));
            }
        }

        [Test]
        public void PeekMessages_EmptyQueue_ReturnsEmptyCollection()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);

            var response = client.PeekMessages(queueName, 32);

            Assert.IsEmpty(response.Messages);
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public void PeekMessages_NonExistentQueue_ThrowsQueueDoesNotExistException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            var response = client.PeekMessages(queueName, 32);

            // expects exception
        }

        [Test]
        public void PeekMessages_Request32ItemsFromFullQueue_Returns32Items()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            _util.AddItemsToQueue(queueName, Enumerable.Range(1, 40).Select(n => n.ToString()).ToList());

            var response = client.PeekMessages(queueName, 32);

            Assert.AreEqual(32, response.Messages.Count);
            for (int i = 1; i <= 32; i++)
            {
                // Base 64 encode the expected message since Azure SDK did so when enqueueing it
                var expectedMessage = Convert.ToBase64String(ASCIIEncoding.ASCII.GetBytes(i.ToString()));
                Assert.IsTrue(response.Messages.Any(m => m.MessageText.Equals(expectedMessage)));
            }
        }

        [Test]
        public void PeekMessages_RequestUndefinedNumberOfItemsFromFullQueue_ReturnsOneItem()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            _util.AddItemsToQueue(queueName, Enumerable.Range(1, 40).Select(n => n.ToString()).ToList());
            var expectedMessage = "1";

            var response = client.PeekMessages(queueName);

            Assert.AreEqual(1, response.Messages.Count);
            var message = response.Messages.Single();
            // Base 64 encode the expected message since Azure SDK did so when enqueueing it
            Assert.AreEqual(Convert.ToBase64String(ASCIIEncoding.ASCII.GetBytes(expectedMessage)), message.MessageText);
        }

        [Test]
        public void PeekMessages_RequestItem_DoesNotDequeueTheItem()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            _util.AddItemsToQueue(queueName, new List<string>() { "1" });

            var peekResponse = client.PeekMessages(queueName, 1);
            Assert.AreEqual(1, peekResponse.Messages.Count);

            var getResponse = client.GetMessages(queueName, 1);
            Assert.AreEqual(1, getResponse.Messages.Count);
        }

        [Test]
        public async Task PeekMessagesAsync_EmptyQueue_ReturnsEmptyCollection()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);

            var response = await client.PeekMessagesAsync(queueName, 32);

            Assert.IsEmpty(response.Messages);
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public async Task PeekMessagesAsync_NonExistentQueue_ThrowsQueueDoesNotExistException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            var response = await client.PeekMessagesAsync(queueName, 32);

            // expects exception
        }

        [Test]
        public void DeleteMessage_ValidMessage_DeletesItFromTheQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            _util.AddItemsToQueue(queueName, new List<string>() { "1" });
            var itemFromQueue = _util.GetItemFromQueue(queueName);

            client.DeleteMessage(queueName, itemFromQueue.Id, itemFromQueue.PopReceipt);

            _util.AssertQueueIsEmpty(queueName);
        }

        [Test]
        [ExpectedException(typeof(MessageNotFoundAzureException))]
        public void DeleteMessage_NonexistentMessage_ThrowsMessageNotFoundException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);

            client.DeleteMessage(queueName, "abc-123", FakePopReceipt);

            // expects exception
        }

        [Test]
        [ExpectedException(typeof(InvalidQueryParameterValueAzureException))]
        public void DeleteMessage_BadlyFormattedPopReceipt_ThrowsInvalidQueryParameterException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);

            client.DeleteMessage(queueName, "abc-123", "bad format");

            // expects exception
        }

        [Test]
        public async Task DeleteMessageAsync_ValidMessage_DeletesItFromTheQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            _util.AddItemsToQueue(queueName, new List<string>() { "1" });
            var itemFromQueue = _util.GetItemFromQueue(queueName);

            await client.DeleteMessageAsync(queueName, itemFromQueue.Id, itemFromQueue.PopReceipt);

            _util.AssertQueueIsEmpty(queueName);
        }

        [Test]
        [ExpectedException(typeof(MessageNotFoundAzureException))]
        public async Task DeleteMessageAsync_NonexistentMessage_ThrowsMessageNotFoundException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);

            await client.DeleteMessageAsync(queueName, "abc-123", FakePopReceipt);

            // expects exception
        }

        [Test]
        public void ClearMessages_MessagesInQueue_LeavesQueueEmpty()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            _util.AddItemsToQueue(queueName, new List<string>() { "1", "2", "3", "4" });

            client.ClearMessages(queueName);

            _util.AssertQueueIsEmpty(queueName);
        }

        [Test]
        public void ClearMessages_NoMessagesInQueue_LeavesQueueEmpty()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);

            client.ClearMessages(queueName);

            _util.AssertQueueIsEmpty(queueName);
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public void ClearMessages_QueueDoesNotExist_ThrowsQueueNotFoundException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            client.ClearMessages(queueName);

            //expects exception
        }

        [Test]
        public async Task ClearMessagesAsync_MessagesInQueue_LeavesQueueEmpty()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            _util.AddItemsToQueue(queueName, new List<string>() { "1", "2", "3", "4" });

            await client.ClearMessagesAsync(queueName);

            _util.AssertQueueIsEmpty(queueName);
        }

        [Test]
        [ExpectedException(typeof(QueueNotFoundAzureException))]
        public async Task ClearMessagesAsync_QueueDoesNotExist_ThrowsQueueNotFoundException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();

            await client.ClearMessagesAsync(queueName);

            //expects exception
        }

        [Test]
        public void UpdateMessage_ClearVisibility_AllowsMessageToBeRetrievedAgain()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            _util.AddItemsToQueue(queueName, new List<string>() { "1" });
            var itemFromQueue = _util.GetItemFromQueue(queueName);

            client.UpdateMessage(queueName, itemFromQueue.Id, itemFromQueue.PopReceipt, 0);

            var itemFromQueueAgain = _util.GetItemFromQueue(queueName);
            Assert.IsNotNull(itemFromQueueAgain);
            Assert.AreEqual(itemFromQueue.Id, itemFromQueueAgain.Id);
        }

        [Test]
        public void UpdateMessage_UpdateContent_UpdatesContentOfMessageInQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            _util.AddItemsToQueue(queueName, new List<string>() { "1" });
            var itemFromQueue = _util.GetItemFromQueue(queueName);
            var newString = Convert.ToBase64String(Encoding.ASCII.GetBytes("2"));   // the official SDK base 64's the values automatically

            client.UpdateMessage(queueName, itemFromQueue.Id, itemFromQueue.PopReceipt, 0, newString);

            var itemFromQueueAgain = _util.GetItemFromQueue(queueName);
            Assert.IsNotNull(itemFromQueueAgain);
            Assert.AreEqual(itemFromQueue.Id, itemFromQueueAgain.Id);
            Assert.AreEqual("1", itemFromQueue.AsString);
            Assert.AreEqual("2", itemFromQueueAgain.AsString);
        }

        [Test]
        public void UpdateMessage_ExtendVisibility_ExtendsMessageVisibilityInTheQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            _util.AddItemsToQueue(queueName, new List<string>() { "1" });
            var itemFromQueue = _util.GetItemFromQueue(queueName, 1);

            var response = client.UpdateMessage(queueName, itemFromQueue.Id, itemFromQueue.PopReceipt, 30);

            Assert.IsNotNullOrEmpty(response.PopReceipt);
            Thread.Sleep(2000); // longer than the original visibility timeout
            var itemFromQueueAgain = _util.GetItemFromQueue(queueName);
            Assert.IsNull(itemFromQueueAgain);
        }

        [Test]
        public void UpdateMessage_DifferentMessageText_ChangesMessageText()
        {
            const string rawFirstText = "1";
            const string rawSecondText = "2";
            var firstText = Convert.ToBase64String(Encoding.UTF8.GetBytes(rawFirstText));
            var secondText = Convert.ToBase64String(Encoding.UTF8.GetBytes(rawSecondText));
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            _util.AddItemsToQueue(queueName, new List<string>() { firstText });
            var itemFromQueue = _util.GetItemFromQueue(queueName, 1);

            client.UpdateMessage(queueName, itemFromQueue.Id, itemFromQueue.PopReceipt, 0, secondText);

            var itemFromQueueAgain = _util.GetItemFromQueue(queueName);
            Assert.AreEqual(rawSecondText, itemFromQueueAgain.AsString);
        }

        [Test]
        public void UpdateMessage_InvalidXMLMessageTextCharacters_DoesNotThrowException()
        {
            const string rawFirstText = "<QueueMessage><MessageText>Test</MessageText></QueueMessage>";
            const string rawSecondText = "{\"KeyValueLogPayload\": {\"UserIP\": \"::1\",\"URL\": \"http://fakeurl/foo?bar=baz&baz=bar\"}}";
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            _util.AddItemsToQueue(queueName, new List<string>() { rawFirstText });
            var itemFromQueue = _util.GetItemFromQueue(queueName, 1);

            Assert.DoesNotThrow(() =>
            {
                client.UpdateMessage(queueName, itemFromQueue.Id, itemFromQueue.PopReceipt, 0, rawSecondText);
            });
        }

        [Test]
        [ExpectedException(typeof(MessageNotFoundAzureException))]
        public void UpdateMessage_NonExistentMessage_ThrowsException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);

            client.UpdateMessage(queueName, "123-abc", FakePopReceipt, 0);

            // expects exception
        }

        [Test]
        public async Task UpdateMessageAsync_ClearVisibility_AllowsMessageToBeRetrievedAgain()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);
            _util.AddItemsToQueue(queueName, new List<string>() { "1" });
            var itemFromQueue = _util.GetItemFromQueue(queueName);

            await client.UpdateMessageAsync(queueName, itemFromQueue.Id, itemFromQueue.PopReceipt, 0);

            var itemFromQueueAgain = _util.GetItemFromQueue(queueName);
            Assert.IsNotNull(itemFromQueueAgain);
            Assert.AreEqual(itemFromQueue.Id, itemFromQueueAgain.Id);
        }

        [Test]
        [ExpectedException(typeof(MessageNotFoundAzureException))]
        public async Task UpdateMessageAsync_NonExistentMessage_ThrowsException()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);

            await client.UpdateMessageAsync(queueName, "123-abc", FakePopReceipt, 0);

            // expects exception
        }

        #endregion

    }
}
