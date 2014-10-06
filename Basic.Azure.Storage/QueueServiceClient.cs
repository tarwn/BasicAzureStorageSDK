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
    public class QueueServiceClient
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

        public void DeleteQueue(string queueName)
        {
            var request = new DeleteQueueRequest(_account, queueName);
            request.Execute();
        }
        public GetQueueMetadataResponse GetQueueMetadata(string queueName)
        {
            var request = new GetQueueMetadataRequest(_account, queueName);
            var response = request.Execute();
            return response.Payload;
        }
        
        public void SetQueueMetadata(string queueName, Dictionary<string, string> metadata)
        {
            var request = new SetQueueMetadataRequest(_account, queueName, metadata);
            request.Execute();
        }

        #endregion

        #region Message Operations

        public void PutMessage(string queueName, string messageData, int? visibilityTimeout = null, int? messageTtl = null)
        {
            var request = new PutMessageRequest(_account, queueName, messageData, visibilityTimeout, messageTtl);
            request.Execute();
        }

        #endregion


    }
}
