using Basic.Azure.Storage.Communications.QueueService;
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

		public void CreateQueue(string queueName, Dictionary<string, string> metadata = null)
		{
			var request = new CreateQueueRequest(_account, queueName, metadata);
			request.Execute();
		}

        public void PutMessage(string queueName, string messageData)
        {
            var request = new PutMessageRequest(_account, queueName, messageData);
            request.Execute();
        }
    }
}
