using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Tests
{
	[TestFixture]
    public class QueueServiceClientTests
    {
        private StorageAccountSettings _accountSettings = new LocalEmulatorAccountSettings();

		private string GenerateSampleQueueName()
		{
			return "unit-test-" + Guid.NewGuid().ToString().ToLower();
		}

		[Test]
		public void CreateQueue_ValidName_CreatesQueue()
		{
			var client = new QueueServiceClient(_accountSettings);
			var queueName = GenerateSampleQueueName();

			client.CreateQueue(queueName);

			AssertQueueExists(queueName);
		}


		private void AssertQueueExists(string queueName) 
		{
			var account = CloudStorageAccount.Parse("UseDevelopmentStorage=true;");
			var client = account.CreateCloudQueueClient();
			var queue = client.GetQueueReference(queueName);
			if (!queue.Exists())
			{
				Assert.Fail(String.Format("The queue '{0}' does not exist", queueName));
			}				
		}
    }
}
