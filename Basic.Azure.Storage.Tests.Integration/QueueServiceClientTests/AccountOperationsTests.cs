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
    public class AccountOperationsTests
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

        #region Account Operations

        [Test]
        public void ListQueues_AtLeastOneQueue_ReturnsListContainingThatQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);

            var response = client.ListQueues();

            Assert.IsTrue(response.Queues.Any(q => q.Name == queueName));
        }

        [Test]
        public void ListQueues_WithPrefix_ReturnsListContainingOnlyQueuesWithThatPrefix()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueNames = new List<string>();
            for (int i = 18; i < 21; i++)
            {
                var queueName = GenerateSampleQueueName(i.ToString());
                _util.CreateQueue(queueName);
                queueNames.Add(queueName);
            }

            var response = client.ListQueues("1");

            Assert.AreEqual("1", response.Prefix);
            Assert.IsTrue(response.Queues.Count(q => q.Name.StartsWith("1")) >= 2);
            Assert.AreEqual(0, response.Queues.Count(q => !q.Name.StartsWith("1")));
        }

        [Test]
        public void ListQueues_MaxResultsSmallerThanQueueList_ReturnsOnlyThatManyResults()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueNames = new List<string>();
            for (int i = 0; i < 5; i++)
            {
                var queueName = GenerateSampleQueueName(i.ToString());
                _util.CreateQueue(queueName);
                queueNames.Add(queueName);
            }

            var response = client.ListQueues(maxResults: 3);

            Assert.AreEqual(3, response.Queues.Count);
        }

        [Test]
        public void ListQueues_WithContinuationMarker_ReturnsRemainderOfList()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueNames = new List<string>();
            for (int i = 0; i < 5; i++)
            {
                var queueName = GenerateSampleQueueName(i.ToString());
                _util.CreateQueue(queueName);
                queueNames.Add(queueName);
            }
            var response = client.ListQueues(maxResults: 3);

            var response2 = client.ListQueues(marker: response.Marker);

            Assert.IsNotEmpty(response2.Queues);
            Assert.GreaterOrEqual(response2.Queues.Count, 2);
        }

        [Test]
        public void ListQueues_IncludingMetadata_ReturnsQueuesWithMetadata()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName, new Dictionary<string, string>() { 
                {"a", "1"},
                {"b", "2"}
            });

            var response = client.ListQueues(includeMetadata: true);

            Assert.IsTrue(response.Queues.Any(q => q.Name == queueName));
            var queue = response.Queues.Where(q => q.Name == queueName).Single();
            Assert.IsNotEmpty(queue.Metadata);
            Assert.AreEqual("1", queue.Metadata["a"]);
            Assert.AreEqual("2", queue.Metadata["b"]);
        }

        [Test]
        public async Task ListQueuesAsync_AtLeastOneQueue_ReturnsListContainingThatQueue()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var queueName = GenerateSampleQueueName();
            _util.CreateQueue(queueName);

            var response = await client.ListQueuesAsync();

            Assert.IsTrue(response.Queues.Any(q => q.Name == queueName));
        }

        [Test]
        public void SetQueueServiceProperties_TurnOffLoggingAndMetrics_SuccessfullyTurnsOffOptionsOnService()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var expectedServiceProperties = new StorageServiceProperties();
            expectedServiceProperties.Logging = new StorageServiceLoggingProperties()
            {
                Delete = false,
                Read = false,
                Write = false,
                RetentionPolicyEnabled = false
            };
            expectedServiceProperties.HourMetrics = new StorageServiceMetricsProperties()
            {
                Enabled = false,
                RetentionPolicyEnabled = false
            };
            expectedServiceProperties.MinuteMetrics = new StorageServiceMetricsProperties()
            {
                Enabled = false,
                RetentionPolicyEnabled = false
            };
            _util.SetServicePropertiesOn();

            client.SetQueueServiceProperties(expectedServiceProperties);

            var actualProperties = _util.GetServiceProperties();
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Shared.Protocol.LoggingOperations.None, actualProperties.Logging.LoggingOperations);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Shared.Protocol.MetricsLevel.None, actualProperties.HourMetrics.MetricsLevel);
        }

        [Test]
        public void SetQueueServiceProperties_TurnOnLoggingAndMetrics_SuccessfullyTurnsOnOptionsOnService()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var expectedServiceProperties = new StorageServiceProperties();
            expectedServiceProperties.Logging = new StorageServiceLoggingProperties()
            {
                Delete = true,
                Read = true,
                Write = true,
                RetentionPolicyEnabled = true,
                RetentionPolicyNumberOfDays = 123
            };
            expectedServiceProperties.HourMetrics = new StorageServiceMetricsProperties()
            {
                Enabled = true,
                IncludeAPIs = true,
                RetentionPolicyEnabled = true,
                RetentionPolicyNumberOfDays = 45
            };
            expectedServiceProperties.MinuteMetrics = new StorageServiceMetricsProperties()
            {
                Enabled = true,
                IncludeAPIs = true,
                RetentionPolicyEnabled = true,
                RetentionPolicyNumberOfDays = 45
            };
            _util.SetServicePropertiesOff();

            client.SetQueueServiceProperties(expectedServiceProperties);

            var actualProperties = _util.GetServiceProperties();
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Shared.Protocol.LoggingOperations.All, actualProperties.Logging.LoggingOperations);
            Assert.AreEqual(123, actualProperties.Logging.RetentionDays);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Shared.Protocol.MetricsLevel.ServiceAndApi, actualProperties.HourMetrics.MetricsLevel);
            Assert.AreEqual(45, actualProperties.HourMetrics.RetentionDays);
        }

        [Test]
        public async Task SetQueueServicePropertiesAsync_TurnOnLoggingAndMetrics_SuccessfullyTurnsOnOptionsOnService()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var expectedServiceProperties = new StorageServiceProperties();
            expectedServiceProperties.Logging = new StorageServiceLoggingProperties()
            {
                Delete = true,
                Read = true,
                Write = true,
                RetentionPolicyEnabled = true,
                RetentionPolicyNumberOfDays = 123
            };
            expectedServiceProperties.HourMetrics = new StorageServiceMetricsProperties()
            {
                Enabled = true,
                IncludeAPIs = true,
                RetentionPolicyEnabled = true,
                RetentionPolicyNumberOfDays = 45
            };
            expectedServiceProperties.MinuteMetrics = new StorageServiceMetricsProperties()
            {
                Enabled = true,
                IncludeAPIs = true,
                RetentionPolicyEnabled = true,
                RetentionPolicyNumberOfDays = 45
            };
            _util.SetServicePropertiesOff();

            await client.SetQueueServicePropertiesAsync(expectedServiceProperties);

            var actualProperties = _util.GetServiceProperties();
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Shared.Protocol.LoggingOperations.All, actualProperties.Logging.LoggingOperations);
            Assert.AreEqual(123, actualProperties.Logging.RetentionDays);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Shared.Protocol.MetricsLevel.ServiceAndApi, actualProperties.HourMetrics.MetricsLevel);
            Assert.AreEqual(45, actualProperties.HourMetrics.RetentionDays);
        }

        [Test]
        public void GetQueueServiceProperties_EverythingEnabled_RetrievesPropertiesSuccessfully()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            _util.SetServicePropertiesOn();

            var response = client.GetQueueServiceProperties();

            Assert.IsTrue(response.Properties.Logging.Delete);
            Assert.IsTrue(response.Properties.Logging.Read);
            Assert.IsTrue(response.Properties.Logging.Write);
            Assert.IsTrue(response.Properties.Logging.RetentionPolicyEnabled);
            Assert.IsTrue(response.Properties.HourMetrics.Enabled);
            Assert.IsTrue(response.Properties.HourMetrics.IncludeAPIs);
            Assert.IsTrue(response.Properties.HourMetrics.RetentionPolicyEnabled);
            Assert.IsTrue(response.Properties.MinuteMetrics.Enabled);
            Assert.IsTrue(response.Properties.MinuteMetrics.IncludeAPIs);
            Assert.IsTrue(response.Properties.MinuteMetrics.RetentionPolicyEnabled);
        }

        [Test]
        public void GetQueueServiceProperties_EverythingDisabled_RetrievesPropertiesSuccessfully()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            _util.SetServicePropertiesOff();

            var response = client.GetQueueServiceProperties();

            Assert.IsFalse(response.Properties.Logging.Delete);
            Assert.IsFalse(response.Properties.Logging.Read);
            Assert.IsFalse(response.Properties.Logging.Write);
            Assert.IsFalse(response.Properties.Logging.RetentionPolicyEnabled);
            Assert.IsFalse(response.Properties.HourMetrics.Enabled);
            Assert.IsFalse(response.Properties.HourMetrics.IncludeAPIs);
            Assert.IsFalse(response.Properties.HourMetrics.RetentionPolicyEnabled);
            Assert.IsFalse(response.Properties.MinuteMetrics.Enabled);
            Assert.IsFalse(response.Properties.MinuteMetrics.IncludeAPIs);
            Assert.IsFalse(response.Properties.MinuteMetrics.RetentionPolicyEnabled);
        }

        [Test]
        public async Task GetQueueServicePropertiesAsync_EverythingEnabled_RetrievesPropertiesSuccessfully()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            _util.SetServicePropertiesOn();

            var response = await client.GetQueueServicePropertiesAsync();

            Assert.IsTrue(response.Properties.Logging.Delete);
            Assert.IsTrue(response.Properties.Logging.Read);
            Assert.IsTrue(response.Properties.Logging.Write);
            Assert.IsTrue(response.Properties.Logging.RetentionPolicyEnabled);
            Assert.IsTrue(response.Properties.HourMetrics.Enabled);
            Assert.IsTrue(response.Properties.HourMetrics.IncludeAPIs);
            Assert.IsTrue(response.Properties.HourMetrics.RetentionPolicyEnabled);
            Assert.IsTrue(response.Properties.MinuteMetrics.Enabled);
            Assert.IsTrue(response.Properties.MinuteMetrics.IncludeAPIs);
            Assert.IsTrue(response.Properties.MinuteMetrics.RetentionPolicyEnabled);
        }

        [Test]
        public void GetQueueServiceProperties_HasCorsRule_ReturnsRule()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var sampleCors = _util.AddSampleToCors();

            var response = client.GetQueueServiceProperties();

            //TODO come back and do better assertions
            Assert.AreEqual(1, response.Properties.Cors.Count);
            Assert.AreEqual(sampleCors.AllowedHeaders.Count, response.Properties.Cors[0].AllowedHeaders.Count);
            //Assert.AreEqual(sampleCors.AllowedMethods.Count, response.Properties.Cors[0].AllowedMethods.Count);
            Assert.AreEqual(sampleCors.AllowedOrigins.Count, response.Properties.Cors[0].AllowedOrigins.Count);
            Assert.AreEqual(sampleCors.ExposedHeaders.Count, response.Properties.Cors[0].ExposedHeaders.Count);
            Assert.AreEqual(sampleCors.MaxAgeInSeconds, response.Properties.Cors[0].MaxAgeInSeconds);
        }

        [Test]
        public async Task GetQueueServicePropertiesAsync_HasCorsRule_ReturnsRule()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var sampleCors = _util.AddSampleToCors();

            var response = await client.GetQueueServicePropertiesAsync();

            //TODO come back and do better assertions
            Assert.AreEqual(1, response.Properties.Cors.Count);
            Assert.AreEqual(sampleCors.AllowedHeaders.Count, response.Properties.Cors[0].AllowedHeaders.Count);
            //Assert.AreEqual(sampleCors.AllowedMethods.Count, response.Properties.Cors[0].AllowedMethods.Count);
            Assert.AreEqual(sampleCors.AllowedOrigins.Count, response.Properties.Cors[0].AllowedOrigins.Count);
            Assert.AreEqual(sampleCors.ExposedHeaders.Count, response.Properties.Cors[0].ExposedHeaders.Count);
            Assert.AreEqual(sampleCors.MaxAgeInSeconds, response.Properties.Cors[0].MaxAgeInSeconds);
        }

        [Test]
        public async Task GetQueueServicePropertiesAsync_HasNoCorsRule_ReturnsRule()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            _util.ClearCorsRules();

            var response = await client.GetQueueServicePropertiesAsync();

            Assert.AreEqual(0, response.Properties.Cors.Count);
        }

        [Test]
        public void SetQueueServiceProperties_AddCorsRule_SuccessfullyAddsCorsRuleToService()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var expectedServiceProperties = new StorageServiceProperties();
            expectedServiceProperties.Cors.Add(new StorageServiceCorsRule() {
                AllowedHeaders = new List<string>() { "X-Whatever" },
                AllowedMethods = new List<string>() {  "GET" },
                AllowedOrigins = new List<string>() {  "a.b.c" },
                ExposedHeaders = new List<string>() {  "X-Whatever" },
                MaxAgeInSeconds = 7
            });
            _util.ClearCorsRules();

            client.SetQueueServiceProperties(expectedServiceProperties);

            var actualProperties = _util.GetServiceProperties();
            Assert.AreEqual(1, actualProperties.Cors.CorsRules.Count);
            Assert.AreEqual("X-Whatever", actualProperties.Cors.CorsRules[0].AllowedHeaders[0]);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Shared.Protocol.CorsHttpMethods.Get, actualProperties.Cors.CorsRules[0].AllowedMethods);
            Assert.AreEqual("a.b.c", actualProperties.Cors.CorsRules[0].AllowedOrigins[0]);
            Assert.AreEqual("X-Whatever", actualProperties.Cors.CorsRules[0].ExposedHeaders[0]);
            Assert.AreEqual(7, actualProperties.Cors.CorsRules[0].MaxAgeInSeconds);
        }

        [Test]
        public async Task SetQueueServicePropertiesAsync_AddCorsRule_SuccessfullyAddsCorsRuleToService()
        {
            IQueueServiceClient client = new QueueServiceClient(_accountSettings);
            var expectedServiceProperties = new StorageServiceProperties();
            expectedServiceProperties.Cors.Add(new StorageServiceCorsRule()
            {
                AllowedHeaders = new List<string>() { "X-Whatever" },
                AllowedMethods = new List<string>() { "GET" },
                AllowedOrigins = new List<string>() { "a.b.c" },
                ExposedHeaders = new List<string>() { "X-Whatever" },
                MaxAgeInSeconds = 7
            });
            _util.ClearCorsRules();

            await client.SetQueueServicePropertiesAsync(expectedServiceProperties);

            var actualProperties = _util.GetServiceProperties();
            Assert.AreEqual(1, actualProperties.Cors.CorsRules.Count);
            Assert.AreEqual("X-Whatever", actualProperties.Cors.CorsRules[0].AllowedHeaders[0]);
            Assert.AreEqual(Microsoft.WindowsAzure.Storage.Shared.Protocol.CorsHttpMethods.Get, actualProperties.Cors.CorsRules[0].AllowedMethods);
            Assert.AreEqual("a.b.c", actualProperties.Cors.CorsRules[0].AllowedOrigins[0]);
            Assert.AreEqual("X-Whatever", actualProperties.Cors.CorsRules[0].ExposedHeaders[0]);
            Assert.AreEqual(7, actualProperties.Cors.CorsRules[0].MaxAgeInSeconds);
        }

        #endregion

    }
}
