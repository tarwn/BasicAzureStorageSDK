using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Tests
{
    [TestFixture]
    public class StorageAccountSettingsTests
    {

        /* Connection String format: https://azure.microsoft.com/en-us/documentation/articles/storage-configure-connection-string/
         * 
         * Development #1: UseDevelopmentStorage=true
         * 
         * Development #2: DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;
         * AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;
         * BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;
         * TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;
         * QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1; 
         * 
         * Development with proxy: UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://myProxyUri
         * 
         * Production Endpoints:
         * AccountName=myAccountName;AccountKey=myAccountKey;
         * 
         * Optional Production Values:
         * DefaultEndpointsProtocol=[http|https];
         * EndpointSuffix=mySuffix;
         * BlobEndpoint=myBlobEndpoint; 
         * QueueEndpoint=myQueueEndpoint;
         * TableEndpoint=myTableEndpoint;
         * SharedAccessSignature=base64Signature (not supported)
         * 
         */

        [Test]
        public void Parse_UseDevelopmentStorage_ReturnsLocalEmulatorSettings()
        {
            string connectionString = "UseDevelopmentStorage=true";

            var result = StorageAccountSettings.Parse(connectionString);

            Assert.IsInstanceOf<LocalEmulatorAccountSettings>(result);
        }

        [Test]
        public void Parse_UseDevelopmentStorageWithproxy_SettingsIncludeProxy()
        {
            string connectionString = "UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://PROXY-HERE";

            var result = StorageAccountSettings.Parse(connectionString);

            Assert.AreEqual("http://PROXY-HERE:10000/devstoreaccount1", result.BlobEndpoint);
            Assert.AreEqual("http://PROXY-HERE:10001/devstoreaccount1", result.QueueEndpoint);
            Assert.AreEqual("http://PROXY-HERE:10002/devstoreaccount1", result.TableEndpoint);
        }

        [Test]
        public void Parse_UseDevelopmentCredentials_ReturnsLocalEmulatorSettings()
        {
            string connectionString = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;";

            var result = StorageAccountSettings.Parse(connectionString);

            Assert.IsInstanceOf<LocalEmulatorAccountSettings>(result);
        }


        [Test]
        public void Parse_UseNonDevelopmentCredentials_ReturnsNormalSettings()
        {
            string connectionString = "AccountName=some-account-name;AccountKey=some-account-key;";

            var result = StorageAccountSettings.Parse(connectionString);

            Assert.IsInstanceOf<StorageAccountSettings>(result);
            Assert.IsNotInstanceOf<LocalEmulatorAccountSettings>(result);
        }

        [Test]
        public void Parse_NoProtocolSpecified_AssumesHttps()
        {
            string connectionString = "AccountName=some-account-name;AccountKey=some-account-key;";

            var result = StorageAccountSettings.Parse(connectionString);

            Assert.IsTrue(result.BlobEndpoint.StartsWith("https://"), "BlobEndpoint should start with https");
            Assert.IsTrue(result.QueueEndpoint.StartsWith("https://"), "QueueEndpoint should start with https");
            Assert.IsTrue(result.TableEndpoint.StartsWith("https://"), "TableEndpoint should start with https");
        }

        [Test]
        public void Parse_HttpsSpecified_UsesHttps()
        {
            string connectionString = "AccountName=some-account-name;AccountKey=some-account-key;";

            var result = StorageAccountSettings.Parse(connectionString);

            Assert.IsTrue(result.BlobEndpoint.StartsWith("https://"), "BlobEndpoint should start with https");
            Assert.IsTrue(result.QueueEndpoint.StartsWith("https://"), "QueueEndpoint should start with https");
            Assert.IsTrue(result.TableEndpoint.StartsWith("https://"), "TableEndpoint should start with https");
        }

        [Test]
        public void Parse_HttpSpecified_UsesHttp()
        {
            string connectionString = "DefaultEndpointsProtocol=http;AccountName=some-account-name;AccountKey=some-account-key;";

            var result = StorageAccountSettings.Parse(connectionString);

            Assert.IsTrue(result.BlobEndpoint.StartsWith("http://"), "BlobEndpoint should start with https");
            Assert.IsTrue(result.QueueEndpoint.StartsWith("http://"), "QueueEndpoint should start with https");
            Assert.IsTrue(result.TableEndpoint.StartsWith("http://"), "TableEndpoint should start with https");
        }

        [Test]
        public void Parse_BlobEndpointSpecified_OverridesDefaultValueForBlobStorage()
        {
            string expectedEndpoint = "http://expected.endpoint.here";
            string connectionString = String.Format("AccountName=some-account-name;AccountKey=some-account-key;BlobEndpoint={0}", expectedEndpoint);

            var result = StorageAccountSettings.Parse(connectionString);

            var defaultStorageSettings = new StorageAccountSettings("some-account-name", "", true);
            Assert.AreEqual(expectedEndpoint, result.BlobEndpoint);
            Assert.AreEqual(defaultStorageSettings.QueueEndpoint, result.QueueEndpoint);
            Assert.AreEqual(defaultStorageSettings.TableEndpoint, result.TableEndpoint);
        }

        [Test]
        public void Parse_QueueEndpointSpecified_OverridesDefaultValueForQueueStorage()
        {
            string expectedEndpoint = "http://expected.endpoint.here";
            string connectionString = String.Format("AccountName=some-account-name;AccountKey=some-account-key;QueueEndpoint={0}", expectedEndpoint);

            var result = StorageAccountSettings.Parse(connectionString);

            var defaultStorageSettings = new StorageAccountSettings("some-account-name", "", true);
            Assert.AreEqual(defaultStorageSettings.BlobEndpoint, result.BlobEndpoint);
            Assert.AreEqual(expectedEndpoint, result.QueueEndpoint);
            Assert.AreEqual(defaultStorageSettings.TableEndpoint, result.TableEndpoint);
        }

        [Test]
        public void Parse_TableEndpointSpecified_OverridesDefaultValueForTableStorage()
        {
            string expectedEndpoint = "http://expected.endpoint.here";
            string connectionString = String.Format("AccountName=some-account-name;AccountKey=some-account-key;TableEndpoint={0}", expectedEndpoint);

            var result = StorageAccountSettings.Parse(connectionString);

            var defaultStorageSettings = new StorageAccountSettings("some-account-name", "", true);
            Assert.AreEqual(defaultStorageSettings.BlobEndpoint, result.BlobEndpoint);
            Assert.AreEqual(defaultStorageSettings.QueueEndpoint, result.QueueEndpoint);
            Assert.AreEqual(expectedEndpoint, result.TableEndpoint);
        }


        [Test]
        public void Parse_EndointSuffixSupplied_UsesProvidedSuffixForAllAddresses()
        {
            string expectedSuffix = "core.expected.address";
            string connectionString = String.Format("AccountName=some-account-name;AccountKey=some-account-key;EndpointSuffix={0}", expectedSuffix);

            var result = StorageAccountSettings.Parse(connectionString);

            Assert.IsTrue(result.BlobEndpoint.EndsWith(expectedSuffix), String.Format("Expected suffix of '{0} but actual value was {1}", expectedSuffix, result.BlobEndpoint));
            Assert.IsTrue(result.QueueEndpoint.EndsWith(expectedSuffix), String.Format("Expected suffix of '{0} but actual value was {1}", expectedSuffix, result.QueueEndpoint));
            Assert.IsTrue(result.TableEndpoint.EndsWith(expectedSuffix), String.Format("Expected suffix of '{0} but actual value was {1}", expectedSuffix, result.TableEndpoint));
        }
    }
}
