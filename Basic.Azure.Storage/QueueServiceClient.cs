using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.QueueService;
using Basic.Azure.Storage.Communications.QueueService.AccountOperations;
using Basic.Azure.Storage.Communications.QueueService.MessageOperations;
using Basic.Azure.Storage.Communications.QueueService.QueueOperations;
using Microsoft.Practices.TransientFaultHandling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage
{
    public class QueueServiceClient : IQueueServiceClient
    {
		private StorageAccountSettings _account;
        private readonly RetryPolicy _optionalRetryPolicy;

		public QueueServiceClient(StorageAccountSettings account, RetryPolicy optionalRetryPolicy = null)
		{
			_account = account;
            _optionalRetryPolicy = optionalRetryPolicy;
		}

        #region Account Operations

        public ListQueuesResponse ListQueues(string prefix = "", int maxResults = 5000, string marker = null, bool includeMetadata = false)
        {
            var request = new ListQueuesRequest(_account, prefix, maxResults, marker, includeMetadata);
            var response = request.Execute(_optionalRetryPolicy);
            return response.Payload;
        }
        public async Task<ListQueuesResponse> ListQueuesAsync(string prefix = "", int maxResults = 5000, string marker = null, bool includeMetadata = false)
        {
            var request = new ListQueuesRequest(_account, prefix, maxResults, marker, includeMetadata);
            var response = await request.ExecuteAsync(_optionalRetryPolicy);
            return response.Payload;
        }

        public void SetQueueServiceProperties(StorageServiceProperties expectedServiceProperties)
        {
            var request = new SetQueueServicePropertiesRequest(_account, expectedServiceProperties);
            request.Execute(_optionalRetryPolicy);
        }
        public async Task SetQueueServicePropertiesAsync(StorageServiceProperties expectedServiceProperties)
        {
            var request = new SetQueueServicePropertiesRequest(_account, expectedServiceProperties);
            await request.ExecuteAsync(_optionalRetryPolicy);
        }

        public GetQueueServicePropertiesResponse GetQueueServiceProperties()
        {
            var request = new GetQueueServicePropertiesRequest(_account);
            var response = request.Execute(_optionalRetryPolicy);
            return response.Payload;
        }
        public async Task<GetQueueServicePropertiesResponse> GetQueueServicePropertiesAsync()
        {
            var request = new GetQueueServicePropertiesRequest(_account);
            var response = await request.ExecuteAsync(_optionalRetryPolicy);
            return response.Payload;
        }

        #endregion

        #region Queue Operations

        public void CreateQueue(string queueName, Dictionary<string, string> metadata = null)
		{
			var request = new CreateQueueRequest(_account, queueName, metadata);
            request.Execute(_optionalRetryPolicy);
		}

        public async Task CreateQueueAsync(string queueName, Dictionary<string, string> metadata = null)
        {
            var request = new CreateQueueRequest(_account, queueName, metadata);
            await request.ExecuteAsync(_optionalRetryPolicy);
        }

        public void DeleteQueue(string queueName)
        {
            var request = new DeleteQueueRequest(_account, queueName);
            request.Execute(_optionalRetryPolicy);
        }
        public async Task DeleteQueueAsync(string queueName)
        {
            var request = new DeleteQueueRequest(_account, queueName);
            await request.ExecuteAsync(_optionalRetryPolicy);
        }

        public GetQueueMetadataResponse GetQueueMetadata(string queueName)
        {
            var request = new GetQueueMetadataRequest(_account, queueName);
            var response = request.Execute(_optionalRetryPolicy);
            return response.Payload;
        }
        public async Task<GetQueueMetadataResponse> GetQueueMetadataAsync(string queueName)
        {
            var request = new GetQueueMetadataRequest(_account, queueName);
            var response = await request.ExecuteAsync(_optionalRetryPolicy);
            return response.Payload;
        }
        
        public void SetQueueMetadata(string queueName, Dictionary<string, string> metadata)
        {
            var request = new SetQueueMetadataRequest(_account, queueName, metadata);
            request.Execute(_optionalRetryPolicy);
        }
        public async Task SetQueueMetadataAsync(string queueName, Dictionary<string, string> metadata)
        {
            var request = new SetQueueMetadataRequest(_account, queueName, metadata);
            await request.ExecuteAsync(_optionalRetryPolicy);
        }

        public GetQueueACLResponse GetQueueACL(string queueName)
        {
            var request = new GetQueueACLRequest(_account, queueName);
            var response = request.Execute(_optionalRetryPolicy);
            return response.Payload;
        }
        public async Task<GetQueueACLResponse> GetQueueACLAsync(string queueName)
        {
            var request = new GetQueueACLRequest(_account, queueName);
            var response = await request.ExecuteAsync(_optionalRetryPolicy);
            return response.Payload;
        }

        public void SetQueueACL(string queueName, List<QueueSignedIdentifier> signedIdentifiers)
        {
            var request = new SetQueueACLRequest(_account, queueName, signedIdentifiers);
            request.Execute(_optionalRetryPolicy);
        }
        public async Task SetQueueACLAsync(string queueName, List<QueueSignedIdentifier> signedIdentifiers)
        {
            var request = new SetQueueACLRequest(_account, queueName, signedIdentifiers);
            await request.ExecuteAsync(_optionalRetryPolicy);
        }
        #endregion

        #region Message Operations

        public void PutMessage(string queueName, string messageData, int? visibilityTimeout = null, int? messageTtl = null)
        {
            var request = new PutMessageRequest(_account, queueName, messageData, visibilityTimeout, messageTtl);
            request.Execute(_optionalRetryPolicy);
        }
        public async Task PutMessageAsync(string queueName, string messageData, int? visibilityTimeout = null, int? messageTtl = null)
        {
            var request = new PutMessageRequest(_account, queueName, messageData, visibilityTimeout, messageTtl);
            await request.ExecuteAsync(_optionalRetryPolicy);
        }

        public GetMessagesResponse GetMessages(string queueName, int numofMessages = 1, int visibilityTimeout = 30, int? messageTtl = null)
        {
            var request = new GetMessagesRequest(_account, queueName, numofMessages, visibilityTimeout, messageTtl);
            var response = request.Execute(_optionalRetryPolicy);
            return response.Payload;
        }
        public async Task<GetMessagesResponse> GetMessagesAsync(string queueName, int numofMessages = 1, int visibilityTimeout = 30, int? messageTtl = null)
        {
            var request = new GetMessagesRequest(_account, queueName, numofMessages, visibilityTimeout, messageTtl);
            var response = await request.ExecuteAsync(_optionalRetryPolicy);
            return response.Payload;
        }

        public PeekMessagesResponse PeekMessages(string queueName, int numofMessages = 1, int? messageTtl = null)
        {
            var request = new PeekMessagesRequest(_account, queueName, numofMessages, messageTtl);
            var response = request.Execute(_optionalRetryPolicy);
            return response.Payload;
        }
        public async Task<PeekMessagesResponse> PeekMessagesAsync(string queueName, int numofMessages = 1, int? messageTtl = null)
        {
            var request = new PeekMessagesRequest(_account, queueName, numofMessages, messageTtl);
            var response = await request.ExecuteAsync(_optionalRetryPolicy);
            return response.Payload;
        }

        public void DeleteMessage(string queueName, string messageId, string popReceipt)
        {
            var request = new DeleteMessageRequest(_account, queueName, messageId, popReceipt);
            request.Execute(_optionalRetryPolicy);
        }
        public async Task DeleteMessageAsync(string queueName, string messageId, string popReceipt)
        {
            var request = new DeleteMessageRequest(_account, queueName, messageId, popReceipt);
            await request.ExecuteAsync(_optionalRetryPolicy);
        }

        public void ClearMessages(string queueName)
        {
            //TODO: implement logic to keep retrying after 500 operation errors until queue is empty
            var request = new ClearMessageRequest(_account, queueName);
            request.Execute(_optionalRetryPolicy);
        }
        public async Task ClearMessagesAsync(string queueName)
        {
            //TODO: implement logic to keep retrying after 500 operation errors until queue is empty
            var request = new ClearMessageRequest(_account, queueName);
            await request.ExecuteAsync(_optionalRetryPolicy);
        }

        public UpdateMessageResponse UpdateMessage(string queueName, string messageId, string popReceipt, int visibilityTimeout = 30, string messageData = null)
        {
            var request = new UpdateMessageRequest(_account, queueName, messageId, popReceipt, visibilityTimeout, messageData);
            var response = request.Execute(_optionalRetryPolicy);
            return response.Payload;
        }
        public async Task<UpdateMessageResponse> UpdateMessageAsync(string queueName, string messageId, string popReceipt, int visibilityTimeout = 30, string messageData = null)
        {
            var request = new UpdateMessageRequest(_account, queueName, messageId, popReceipt, visibilityTimeout, messageData);
            var response = await request.ExecuteAsync(_optionalRetryPolicy);
            return response.Payload;
        }
        #endregion

    }
}
