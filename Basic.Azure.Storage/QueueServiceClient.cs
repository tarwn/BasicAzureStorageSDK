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

		public void CreateQueue(string queueName)
		{
			var request = new CreateQueueRequest(_account, queueName);
			request.Execute();
		}
	}
}
