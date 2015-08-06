using System;
using System.Collections.Generic;
using System.Net;
using Basic.Azure.Storage.Communications.BlobService.BlobOperations;
using Basic.Azure.Storage.Tests.Fakes;
using NUnit.Framework;
using TestableHttpWebResponse;
using TestableHttpWebResponse.ResponseSettings;

namespace Basic.Azure.Storage.Tests.Communications.BlobService.BlobOperations
{
    [TestFixture]
    public class DeleteBlobRequestTests
    {
        private StorageAccountSettings _settings;

        [TestFixtureSetUp]
        public void SetupFixture()
        {
            _settings = new SettingsFake();
            WebRequest.RegisterPrefix("test", TestableWebRequestCreateFactory.GetFactory());
        }

        [Test]
        public void Execute_DeleteBlob_ResponseParsesHeadersCorrectly()
        {
            var expectedContainer = "test-container";
            var expectedBlob = "test-blob";
            var expectedUri = String.Format("{0}/{1}/{2}", _settings.BlobEndpoint, expectedContainer, expectedBlob);
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri))
                                            .EnqueueResponse(new HttpResponseSettings((HttpStatusCode)202, "Created", "", false, new Dictionary<string, string>(){
                                                {"Date", DateTime.UtcNow.ToString() }
                                            }));
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var expectedDate = DateTime.UtcNow;
            var request = new DeleteBlobRequest(_settings, expectedContainer, expectedBlob);

            var response = request.Execute();

            Assert.AreEqual(response.HttpStatus, HttpStatusCode.Accepted);
            Assert.IsTrue(Math.Abs(expectedDate.Subtract(response.Payload.Date).TotalMinutes) < 1);
        }


    }
}
