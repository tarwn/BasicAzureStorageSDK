using Basic.Azure.Storage.Communications.BlobService.BlobOperations;
using Basic.Azure.Storage.Tests.Fakes;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Net;
using Basic.Azure.Storage.Communications.Common;
using TestableHttpWebResponse;
using TestableHttpWebResponse.ResponseSettings;

namespace Basic.Azure.Storage.Tests.Communications.BlobService.BlobOperations
{
    [TestFixture]
    public class CopyBlobRequestTests
    {
        private StorageAccountSettings _settings;

        [TestFixtureSetUp]
        public void SetupFixture()
        {
            _settings = new SettingsFake();
            WebRequest.RegisterPrefix("test", TestableWebRequestCreateFactory.GetFactory());
        }

        [Test]
        public void Execute_CopyBlobWithAllHeaders_AllHeadersSetProperly()
        {
            const string expectedContainer = "test-container";
            const string expectedBlob = "test-blob";
            const string copySource = "http://foo.foo.foo/testContainer/testBlob";
            var expectedUri = String.Format("{0}/{1}/{2}", _settings.BlobEndpoint, expectedContainer, expectedBlob);
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri))
                                            .EnqueueResponse(new HttpResponseSettings((HttpStatusCode)202, "Accepted", "", false, new Dictionary<string,string>(){
                                                {"ETag", "\"123\""},
                                                {"Date", DateTime.UtcNow.ToString() },
                                                {"Last-Modified", DateTime.UtcNow.ToString() },
                                                {"x-ms-copy-id", "123-copy-id" },
                                                {"x-ms-copy-status", "success" }
                                            }));
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var request = new CopyBlobRequest(_settings, expectedContainer, expectedBlob, copySource);
            
            request.Execute();

            Assert.AreEqual(copySource, expectedRawRequest.Headers["x-ms-copy-source"]);
        }

        [Test]
        public void Execute_CopyBlob_ResponseParsesHeadersCorrectly()
        {
            const string expectedContainer = "test-container";
            const string expectedBlob = "test-blob";
            const string copySource = "http://foo.foo.foo/testContainer/testBlob";
            var expectedUri = String.Format("{0}/{1}/{2}", _settings.BlobEndpoint, expectedContainer, expectedBlob);
            var expectedDate = DateTime.UtcNow.AddDays(-2345);
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri))
                                            .EnqueueResponse(new HttpResponseSettings((HttpStatusCode)201, "Success", "", false, new Dictionary<string, string>(){
                                                {"ETag", "\"123\""},
                                                {"Date", expectedDate.ToString() },
                                                {"Last-Modified", expectedDate.ToString() },
                                                {"x-ms-copy-id", "123-copy-id" },
                                                {"x-ms-copy-status", "success" }
                                            }));
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var request = new CopyBlobRequest(_settings, expectedContainer, expectedBlob, copySource);

            var response = request.Execute();

            Assert.AreEqual("123", response.Payload.ETag);
            Assert.IsTrue(Math.Abs(expectedDate.Subtract(response.Payload.Date).TotalMinutes) < 1);
            Assert.IsTrue(Math.Abs(expectedDate.Subtract(response.Payload.LastModified).TotalMinutes) < 1);
            Assert.AreEqual("123-copy-id", response.Payload.CopyId);
            Assert.AreEqual(CopyStatus.Success, response.Payload.CopyStatus);
        }

    }
}
