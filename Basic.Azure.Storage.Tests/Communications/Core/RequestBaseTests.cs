using Basic.Azure.Storage.Communications;
using Basic.Azure.Storage.Communications.Core;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using TestableHttpWebResponse;
using TestableHttpWebResponse.ResponseSettings;

namespace Basic.Azure.Storage.Tests.Communications.Core
{
    [TestFixture]
    public class RequestBaseTests
    {
        [TestFixtureSetUp]
        public void SetupFixture()
        {
            WebRequest.RegisterPrefix("test", TestableWebRequestCreateFactory.GetFactory());
        }

        [Test]
        public void Execute_ExpectingEmptyPayload_ConsumesResponseStream()
        {
            var expectedUri = "test://queue.abc/whatever/";
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri));
            var expectedRawResponse = new HttpResponseSettings(HttpStatusCode.OK, "Success", "Response content", false);
            expectedRawRequest.EnqueueResponse(expectedRawResponse);
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var request = new RequestWithEmptyPayload(new SettingsFake(), expectedUri, "GET");

            var response = request.Execute();

            // expect the response stream to be closed now
            long x;
            Assert.Throws<ObjectDisposedException>(() => x = expectedRawResponse.ResponseStream.Position);
        }

        [Test]
        public void Execute_SuccessfulOperation_MarksResponseAsSuccessfull()
        {
            var expectedUri = "test://queue.abc/whatever/";
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri));
            expectedRawRequest.EnqueueResponse(HttpStatusCode.OK, "Success", "Response content", false);
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var request = new RequestWithEmptyPayload(new SettingsFake(), expectedUri, "GET");

            var response = request.Execute();

            Assert.AreEqual(HttpStatusCode.OK, response.Status);
        }

        #region Retryable Error Cases

        [TestCase("InternalError", 500, "The server encountered an internal error. Please retry the request.")]
        [TestCase("ServerBusy", 503, "The server is currently unable to receive requests. Please retry your request.")]
        [TestCase("OperationTimedOut", 500, "The operation could not be completed within the permitted time.")]
        public void Execute_ReceivesStatusError_AttemptsToRetry(string errorCode, int httpStatus, string errorMessage)
        {
            var expectedUri = "test://queue.abc/whatever/";
            var azureErrorMessage = String.Format("<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>{0}</Code><Message>{1}</Message></Error>", errorCode, errorMessage);
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri))
                                        .EnqueueResponse((HttpStatusCode) httpStatus, errorCode, azureErrorMessage, true)
                                        .EnqueueResponse(HttpStatusCode.OK, "Success", "Response content", false);
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var request = new RequestWithEmptyPayload(new SettingsFake(), expectedUri, "GET");

            var response = request.Execute();

            Assert.AreEqual(2, response.NumberOfAttempts);
        }

        #endregion

    }

    public class RequestWithEmptyPayload : RequestBase<EmptyResponsePayload>
    {

        public RequestWithEmptyPayload(StorageAccountSettings settings, string uri, string httpMethod)
            : base(settings)
        {
            TestRequestUri = uri;
            TestHttpMethod = httpMethod;
        }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(TestRequestUri);
            return builder;
        }

        protected override string HttpMethod { get { return TestHttpMethod; } }

        protected override AuthenticationMethod AuthenticationMethod
        {
            get { return Storage.Communications.Core.AuthenticationMethod.SharedKeyForBlobAndQueueServices; }
        }

        protected override void ApplyRequiredHeaders(WebRequest request)
        { }

        protected override void ApplyOptionalHeaders(WebRequest request)
        { }

        public string TestHttpMethod { get; protected set; }
        public string TestRequestUri { get; protected set; }
    }

    public class SettingsFake : StorageAccountSettings
    {
        public static string FakeKey = Convert.ToBase64String(Encoding.ASCII.GetBytes("unit-test"));

        public SettingsFake()
            : base("unit-test", FakeKey, false)
        { }

        public override string BlobEndpoint { get { return "http://blob.abc"; } }
        public override string QueueEndpoint { get { return "http://queue.abc"; } }
        public override string TableEndpoint { get { return "http://table.abc"; } }

    }
}
