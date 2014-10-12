using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.QueueService;
using Basic.Azure.Storage.Communications.QueueService.MessageOperations;
using Basic.Azure.Storage.Communications.QueueService.QueueOperations;
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

		public QueueServiceClient(StorageAccountSettings account)
		{
			_account = account;
		}

        #region Queue Operations

        public void CreateQueue(string queueName, Dictionary<string, string> metadata = null)
		{
			var request = new CreateQueueRequest(_account, queueName, metadata);
			request.Execute();
		}

        public async Task CreateQueueAsync(string queueName, Dictionary<string, string> metadata = null)
        {
            var request = new CreateQueueRequest(_account, queueName, metadata);
            await request.ExecuteAsync();
        }

        public void DeleteQueue(string queueName)
        {
            var request = new DeleteQueueRequest(_account, queueName);
            request.Execute();
        }
        public async Task DeleteQueueAsync(string queueName)
        {
            var request = new DeleteQueueRequest(_account, queueName);
            await request.ExecuteAsync();
        }

        public GetQueueMetadataResponse GetQueueMetadata(string queueName)
        {
            var request = new GetQueueMetadataRequest(_account, queueName);
            var response = request.Execute();
            return response.Payload;
        }
        public async Task<GetQueueMetadataResponse> GetQueueMetadataAsync(string queueName)
        {
            var request = new GetQueueMetadataRequest(_account, queueName);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }
        
        public void SetQueueMetadata(string queueName, Dictionary<string, string> metadata)
        {
            var request = new SetQueueMetadataRequest(_account, queueName, metadata);
            request.Execute();
        }
        public async Task SetQueueMetadataAsync(string queueName, Dictionary<string, string> metadata)
        {
            var request = new SetQueueMetadataRequest(_account, queueName, metadata);
            await request.ExecuteAsync();
        }

        public GetQueueACLResponse GetQueueACL(string queueName)
        {
            var request = new GetQueueACLRequest(_account, queueName);
            var response = request.Execute();
            return response.Payload;
        }
        public async Task<GetQueueACLResponse> GetQueueACLAsync(string queueName)
        {
            var request = new GetQueueACLRequest(_account, queueName);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }

        public void SetQueueACL(string queueName, List<SignedIdentifier> signedIdentifiers)
        {
            var request = new SetQueueACLRequest(_account, queueName, signedIdentifiers);
            request.Execute();
        }
        public async Task SetQueueACLAsync(string queueName, List<SignedIdentifier> signedIdentifiers)
        {
            var request = new SetQueueACLRequest(_account, queueName, signedIdentifiers);
            await request.ExecuteAsync();
        }
        #endregion

        #region Message Operations

        public void PutMessage(string queueName, string messageData, int? visibilityTimeout = null, int? messageTtl = null)
        {
            var request = new PutMessageRequest(_account, queueName, messageData, visibilityTimeout, messageTtl);
            request.Execute();
        }
        public async Task PutMessageAsync(string queueName, string messageData, int? visibilityTimeout = null, int? messageTtl = null)
        {
            var request = new PutMessageRequest(_account, queueName, messageData, visibilityTimeout, messageTtl);
            await request.ExecuteAsync();
        }

        public GetMessagesResponse GetMessages(string queueName, int numofMessages = 1, int visibilityTimeout = 30, int? messageTtl = null)
        {
            var request = new GetMessagesRequest(_account, queueName, numofMessages, visibilityTimeout, messageTtl);
            var response = request.Execute();
            return response.Payload;
        }
        public async Task<GetMessagesResponse> GetMessagesAsync(string queueName, int numofMessages = 1, int visibilityTimeout = 30, int? messageTtl = null)
        {
            var request = new GetMessagesRequest(_account, queueName, numofMessages, visibilityTimeout, messageTtl);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }

        public PeekMessagesResponse PeekMessages(string queueName, int numofMessages = 1, int visibilityTimeout = 30, int? messageTtl = null)
        {
            var request = new PeekMessagesRequest(_account, queueName, numofMessages, visibilityTimeout, messageTtl);
            var response = request.Execute();
            return response.Payload;
        }
        public async Task<PeekMessagesResponse> PeekMessagesAsync(string queueName, int numofMessages = 1, int visibilityTimeout = 30, int? messageTtl = null)
        {
            var request = new PeekMessagesRequest(_account, queueName, numofMessages, visibilityTimeout, messageTtl);
            var response = await request.ExecuteAsync();
            return response.Payload;
        }
        #endregion



    }
}
