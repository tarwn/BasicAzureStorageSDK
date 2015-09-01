using Basic.Azure.Storage.Communications.BlobService.BlobOperations;
using Basic.Azure.Storage.Tests.Fakes;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using TestableHttpWebResponse;
using TestableHttpWebResponse.ResponseSettings;

namespace Basic.Azure.Storage.Tests.Communications.BlobService.BlobOperations
{
    [TestFixture]
    public class PutBlobRequestTests
    {
        private StorageAccountSettings _settings;

        [TestFixtureSetUp]
        public void SetupFixture()
        {
            _settings = new SettingsFake();
            WebRequest.RegisterPrefix("test", TestableWebRequestCreateFactory.GetFactory());
        }

        [Test]
        public void Execute_BlockBlobWithAllHeaders_AllHeadersSetProperly()
        {
            var expectedContainer = "test-container";
            var expectedBlob = "test-blob";
            var expectedUri = String.Format("{0}/{1}/{2}", _settings.BlobEndpoint, expectedContainer, expectedBlob);
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri))
                                            .EnqueueResponse(new HttpResponseSettings((HttpStatusCode)201, "Created", "", false, new Dictionary<string,string>(){
                                                {"ETag", "\"123\""},
                                                {"Date", DateTime.UtcNow.ToString() },
                                                {"Last-Modified", DateTime.UtcNow.ToString() },
                                                {"Content-MD5", "123-MD5" }
                                            }));
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var expectedData = UTF8Encoding.UTF8.GetBytes("test-data");
            var request = new PutBlobRequest(_settings, expectedContainer, expectedBlob, expectedData, 
                                            "content/type", "content/encoding", "content/language", 
                                            "content/md5", "cache/control", null);  

            var response = request.Execute();

            Assert.AreEqual(expectedData.Length, expectedRawRequest.ContentLength);
            Assert.AreEqual("content/type", expectedRawRequest.ContentType);
            Assert.AreEqual("content/encoding", expectedRawRequest.Headers["Content-Encoding"]);
            Assert.AreEqual("content/language", expectedRawRequest.Headers["Content-Language"]);
            Assert.AreEqual("content/md5", expectedRawRequest.Headers["Content-MD5"]);
            Assert.AreEqual("cache/control", expectedRawRequest.Headers["Cache-Control"]);
        }

        [Test]
        public void Execute_PageBlobWithAllHeaders_AllHeadersSetProperly()
        {
            var expectedContainer = "test-container";
            var expectedBlob = "test-blob";
            var expectedUri = String.Format("{0}/{1}/{2}", _settings.BlobEndpoint, expectedContainer, expectedBlob);
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri))
                                            .EnqueueResponse(new HttpResponseSettings((HttpStatusCode)201, "Created", "", false, new Dictionary<string, string>(){
                                                {"ETag", "\"123\""},
                                                {"Date", DateTime.UtcNow.ToString() },
                                                {"Last-Modified", DateTime.UtcNow.ToString() },
                                                {"Content-MD5", "123-MD5" }
                                            }));
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var request = new PutBlobRequest(_settings, expectedContainer, expectedBlob, 12345,
                                            "content/type", "content/encoding", "content/language",
                                            "content/md5", "cache/control", null, 321);

            var response = request.Execute();

            Assert.AreEqual(0, expectedRawRequest.ContentLength);
            Assert.AreEqual("12345", expectedRawRequest.Headers["x-ms-blob-content-length"]);
            Assert.AreEqual("content/type", expectedRawRequest.ContentType);
            Assert.AreEqual("content/encoding", expectedRawRequest.Headers["Content-Encoding"]);
            Assert.AreEqual("content/language", expectedRawRequest.Headers["Content-Language"]);
            Assert.AreEqual("content/md5", expectedRawRequest.Headers["Content-MD5"]);
            Assert.AreEqual("cache/control", expectedRawRequest.Headers["Cache-Control"]);
            Assert.AreEqual("321", expectedRawRequest.Headers["x-ms-blob-sequence-number"]);
        }

        [Test]
        public void Execute_BlockBlob_ResponseParsesHeadersCorrectly()
        {
            var expectedContainer = "test-container";
            var expectedBlob = "test-blob";
            var expectedUri = String.Format("{0}/{1}/{2}", _settings.BlobEndpoint, expectedContainer, expectedBlob);
            var expectedDate = DateTime.UtcNow.AddDays(-2345);
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri))
                                            .EnqueueResponse(new HttpResponseSettings((HttpStatusCode)201, "Success", "", false, new Dictionary<string, string>(){
                                                {"ETag", "\"123\""},
                                                {"Date", expectedDate.ToString() },
                                                {"Last-Modified", expectedDate.ToString() },
                                                {"Content-MD5", "123-MD5" }
                                            }));
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var expectedData = UTF8Encoding.UTF8.GetBytes("test-data");
            var request = new PutBlobRequest(_settings, expectedContainer, expectedBlob, expectedData);

            var response = request.Execute();

            Assert.AreEqual("123", response.Payload.ETag);
            Assert.IsTrue(Math.Abs(expectedDate.Subtract(response.Payload.Date).TotalMinutes) < 1);
            Assert.IsTrue(Math.Abs(expectedDate.Subtract(response.Payload.LastModified).TotalMinutes) < 1);
            Assert.AreEqual("123-MD5", response.Payload.ContentMD5);
        }


    }
}
