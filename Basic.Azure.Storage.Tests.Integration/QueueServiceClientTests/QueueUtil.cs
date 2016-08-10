using Basic.Azure.Storage.Communications.Common;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue.Protocol;
using Microsoft.WindowsAzure.Storage.Shared.Protocol;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Tests.Integration.QueueServiceClientTests
{
    public class QueueUtil
    {
        public CloudStorageAccount _storageAccount;

        public QueueUtil(string connectionString)
        {
            _storageAccount = CloudStorageAccount.Parse(connectionString);
        }

        public DateTime GetTruncatedUtcNow()
        {
            return new DateTime(DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day, DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, DateTime.UtcNow.Second, DateTimeKind.Utc);
        }

        #region Setup Mechanics

        public void CreateQueue(string queueName, Dictionary<string, string> metadata = null)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            queue.Create();

            if (metadata != null)
            {
                // tell me how this method of adding metadata makes any sense at all?
                foreach (var key in metadata.Keys)
                {
                    queue.Metadata.Add(key, metadata[key]);
                }
                queue.SetMetadata();
            }
        }

        public void AddItemsToQueue(string queueName, List<string> messages)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);

            foreach (var message in messages)
            {
                queue.AddMessage(new Microsoft.WindowsAzure.Storage.Queue.CloudQueueMessage(Encoding.ASCII.GetBytes(message)));
            }
        }

        public Microsoft.WindowsAzure.Storage.Queue.CloudQueueMessage GetItemFromQueue(string queueName, int visibility = 30)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);

            return queue.GetMessage(TimeSpan.FromSeconds(visibility));
        }

        public IDictionary<string, string> GetQueueMetadata(string queueName)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);

            queue.FetchAttributes();
            return queue.Metadata;
        }

        public void AddAccessPolicy(string queueName, string id, DateTime startDate, DateTime expiry)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            var permissions = queue.GetPermissions();
            permissions.SharedAccessPolicies.Add(id, new Microsoft.WindowsAzure.Storage.Queue.SharedAccessQueuePolicy()
            {
                Permissions = Microsoft.WindowsAzure.Storage.Queue.SharedAccessQueuePermissions.Add,
                SharedAccessStartTime = startDate,
                SharedAccessExpiryTime = expiry
            });
            queue.SetPermissions(permissions);
        }

        public QueuePermissions GetQueuePermissions(string queueName)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            var permissions = queue.GetPermissions();
            return permissions;
        }

        public void SetServiceProperties(Microsoft.WindowsAzure.Storage.Shared.Protocol.LoggingOperations logging, int? loggingRetention,
                                            Microsoft.WindowsAzure.Storage.Shared.Protocol.MetricsLevel hourMetrics, int? hourRetention,
                                            Microsoft.WindowsAzure.Storage.Shared.Protocol.MetricsLevel minuteMetrics, int? minuteRetention)
        {
            var cloudClient = _storageAccount.CreateCloudQueueClient();
            var actualProperties = cloudClient.GetServiceProperties();
            actualProperties.Logging.LoggingOperations = logging;
            actualProperties.Logging.RetentionDays = loggingRetention;
            actualProperties.HourMetrics.MetricsLevel = hourMetrics;
            actualProperties.HourMetrics.RetentionDays = hourRetention;
            actualProperties.MinuteMetrics.MetricsLevel = minuteMetrics;
            actualProperties.MinuteMetrics.RetentionDays = minuteRetention;
            cloudClient.SetServiceProperties(actualProperties);
        }

        public void SetServicePropertiesOn()
        {
            SetServiceProperties(Microsoft.WindowsAzure.Storage.Shared.Protocol.LoggingOperations.All, 7,
                                Microsoft.WindowsAzure.Storage.Shared.Protocol.MetricsLevel.ServiceAndApi, 7,
                                Microsoft.WindowsAzure.Storage.Shared.Protocol.MetricsLevel.ServiceAndApi, 7);
        }

        public void SetServicePropertiesOff()
        {
            SetServiceProperties(Microsoft.WindowsAzure.Storage.Shared.Protocol.LoggingOperations.None, null,
                                Microsoft.WindowsAzure.Storage.Shared.Protocol.MetricsLevel.None, null,
                                Microsoft.WindowsAzure.Storage.Shared.Protocol.MetricsLevel.None, null);
        }


        public CorsRule AddSampleToCors()
        {
            var cloudClient = _storageAccount.CreateCloudQueueClient();
            var actualProperties = cloudClient.GetServiceProperties();
            actualProperties.Cors = new Microsoft.WindowsAzure.Storage.Shared.Protocol.CorsProperties();
            actualProperties.Cors.CorsRules.Add(new Microsoft.WindowsAzure.Storage.Shared.Protocol.CorsRule()
            {
                AllowedOrigins = new List<string>() { "a.com", "b.com" },
                AllowedMethods = Microsoft.WindowsAzure.Storage.Shared.Protocol.CorsHttpMethods.Get,
                MaxAgeInSeconds = 7,
                ExposedHeaders = new List<string>() { "Content-Type", "Content-Length" },
                AllowedHeaders = new List<string>() { "Content-Type", "Content-Length" }
            });

            cloudClient.SetServiceProperties(actualProperties);
            return actualProperties.Cors.CorsRules[0];
        }
        
        public void ClearCorsRules()
        {
            var cloudClient = _storageAccount.CreateCloudQueueClient();
            var actualProperties = cloudClient.GetServiceProperties();
            actualProperties.Cors = new Microsoft.WindowsAzure.Storage.Shared.Protocol.CorsProperties();
            cloudClient.SetServiceProperties(actualProperties);
        }

        #endregion

        #region Assertions

        public void AssertQueueExists(string queueName)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            if (!queue.Exists())
            {
                Assert.Fail(String.Format("The queue '{0}' does not exist", queueName));
            }
        }

        public void AssertQueueDoesNotExist(string queueName)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);
            if (queue.Exists())
            {
                Assert.Fail(String.Format("The queue '{0}' exists", queueName));
            }
        }

        public void AssertQueueHasMessage(string queueName)
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

        public void AssertQueueIsEmpty(string queueName)
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

        public void AssertQueueInvisibleMessage(string queueName)
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

        public void AssertIdentifierInSharedAccessPolicies(Microsoft.WindowsAzure.Storage.Queue.SharedAccessQueuePolicies sharedAccessPolicies, QueueSignedIdentifier expectedIdentifier)
        {
            var policy = sharedAccessPolicies.Where(i => i.Key.Equals(expectedIdentifier.Id, StringComparison.InvariantCultureIgnoreCase)).FirstOrDefault();
            Assert.IsNotNull(policy);
            Assert.AreEqual(expectedIdentifier.AccessPolicy.StartTime, policy.Value.SharedAccessStartTime.Value.UtcDateTime);
            Assert.AreEqual(expectedIdentifier.AccessPolicy.Expiry, policy.Value.SharedAccessExpiryTime.Value.UtcDateTime);
            Assert.IsTrue(policy.Value.Permissions.HasFlag(Microsoft.WindowsAzure.Storage.Queue.SharedAccessQueuePermissions.Add));
        }


        #endregion

        #region Cleanup

        public void DeleteIfExists(List<string> queueNames)
        {
            var client = _storageAccount.CreateCloudQueueClient();
            foreach (var queueName in queueNames)
            {
                var queue = client.GetQueueReference(queueName);
                queue.DeleteIfExists();
            }
        }

        #endregion

        public ServiceProperties GetServiceProperties()
        {
            var cloudClient = _storageAccount.CreateCloudQueueClient();
            var actualProperties = cloudClient.GetServiceProperties();
            return actualProperties;
        }

    }
}
