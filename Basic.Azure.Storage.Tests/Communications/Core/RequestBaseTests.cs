using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Tests.Fakes;
using Microsoft.Practices.TransientFaultHandling;
using NUnit.Framework;
using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using TestableHttpWebResponse;
using TestableHttpWebResponse.ResponseSettings;

namespace Basic.Azure.Storage.Tests.Communications.Core
{
    [TestFixture]
    public class RequestBaseTests
    {
        private const string queueAlreadyExistsExceptionText = 
@"<?xml version=""1.0"" encoding=""utf-8""?>
<Error>
   <Code>QueueAlreadyExists</Code>
   <Message>The specified queue does not exist.
   RequestId:fda2babe-ffbb-4f0e-a11b-2bfbd871ba9f
   Time:2012-05-02T17:47:55.4334169Z</Message>
</Error>";

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

            Assert.AreEqual(HttpStatusCode.OK, response.HttpStatus);
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
                                        .EnqueueResponse((HttpStatusCode)httpStatus, errorCode, azureErrorMessage, true)
                                        .EnqueueResponse(HttpStatusCode.OK, "Success", "Response content", false);
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var request = new RequestWithEmptyPayload(new SettingsFake(), expectedUri, "GET");

            var response = request.Execute();

            Assert.AreEqual(2, response.NumberOfAttempts);
        }

        [Test]
        public void Execute_TimeoutException_AttemptsToRetry()
        {
            var expectedUri = "test://queue.abc/whatever/";
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri))
                                        .EnqueueResponse(new TimeoutException("message"))
                                        .EnqueueResponse(HttpStatusCode.OK, "Success", "Even More Success", false);
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var request = new RequestWithEmptyPayload(new SettingsFake(), expectedUri, "GET");

            var response = request.Execute();

            Assert.AreEqual(2, response.NumberOfAttempts);
        }

        [Test]
        public void Execute_ConnectFailureException_AttemptsToRetry()
        {
            var expectedUri = "test://queue.abc/whatever/";
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri))
                                        .EnqueueResponse(new WebException("Could not connect because Azure crashed", new SocketException(1), WebExceptionStatus.ConnectFailure, null))
                                        .EnqueueResponse(HttpStatusCode.OK, "Success", "Even More Success", false);
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var request = new RequestWithEmptyPayload(new SettingsFake(), expectedUri, "GET");

            var response = request.Execute();

            Assert.AreEqual(2, response.NumberOfAttempts);
        }

        [Test]
        public void Execute_ReceiveFailureException_AttemptsToRetry()
        {
            var expectedUri = "test://queue.abc/whatever/";
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri))
                                        .EnqueueResponse(new WebException("Azure hung up after the initial request", new SocketException(1), WebExceptionStatus.ReceiveFailure, null))
                                        .EnqueueResponse(HttpStatusCode.OK, "Success", "Even More Success", false);
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var request = new RequestWithEmptyPayload(new SettingsFake(), expectedUri, "GET");

            var response = request.Execute();

            Assert.AreEqual(2, response.NumberOfAttempts);
        }

        [Test]
        [ExpectedException(typeof(QueueAlreadyExistsAzureException))]
        public void Execute_ReceiveTransientExceptionThenNonTransient_ThrowsNonTransient()
        {
            var expectedUri = "test://queue.abc/whatever/";
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri))
                .EnqueueResponse(new WebException("Azure hung up after the initial request", new SocketException(1), WebExceptionStatus.ReceiveFailure, null))
                .EnqueueResponse(HttpStatusCode.Conflict, "QueueAlreadyExists", queueAlreadyExistsExceptionText, true);
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var request = new RequestWithEmptyPayload(new SettingsFake(), expectedUri, "GET");

            request.Execute();

            // expected non transient exception
        }

        [Test]
        public void Execute_ReceiveTransientExceptionThenNonTransient_ThrowsExceptionWithProperRetryStack()
        {
            var expectedUri = "test://queue.abc/whatever/";
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri))
                .EnqueueResponse(new WebException("Azure hung up after the initial request", new SocketException(1), WebExceptionStatus.ReceiveFailure, null))
                .EnqueueResponse(HttpStatusCode.Conflict, "QueueAlreadyExists", queueAlreadyExistsExceptionText, true);
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var request = new RequestWithEmptyPayload(new SettingsFake(), expectedUri, "GET");

            try
            {
                request.Execute();
            }
            catch (QueueAlreadyExistsAzureException exc)
            {

                Assert.AreEqual(2, exc.RetryCount);
                Assert.AreEqual(exc.RetryCount, exc.ExceptionRetryStack.Count);
                Assert.IsInstanceOf<QueueAlreadyExistsAzureException>(exc.ExceptionRetryStack.First());
                Assert.IsInstanceOf<UnidentifiedAzureException>(exc.ExceptionRetryStack.Last());
                Assert.IsInstanceOf<WebException>(exc.ExceptionRetryStack.Last().InnerException);
            }
        }

        [Test]
        public void Execute_FailsUntilRetryCountExceeded_ThrowsExceptionWithProperRetryStack()
        {
            var expectedUri = "test://queue.abc/whatever/";
            const string message = "message";
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri))
                .EnqueueResponse(new TimeoutException(message + "1"))
                .EnqueueResponse(new TimeoutException(message + "2"))
                .EnqueueResponse(new TimeoutException(message + "3"));
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var request = new RequestWithEmptyPayload(new SettingsFake(), expectedUri, "GET");
            request.RetryPolicy = new RetryPolicy<ExceptionRetryStrategy>(2, TimeSpan.FromMilliseconds(1));
            try
            {

                // Act
                request.Execute();

                //Assert, starting with catching the specific exception type
            }
            catch (RetriedException exc)
            {
                Assert.AreEqual(3, exc.Count);
                Assert.AreEqual(exc.Count, exc.ExceptionRetryStack.Count);
                var currentExceptionIndex = 3;
                foreach (var exception in exc.ExceptionRetryStack)
                {
                    Assert.IsInstanceOf<GeneralExceptionDuringAzureOperationException>(exception);
                    Assert.IsInstanceOf<TimeoutException>(exception.InnerException);
                    Assert.AreEqual(exception.InnerException.Message, message + currentExceptionIndex);
                    currentExceptionIndex--; // stack is LIFO
                }
            }
        }

        [Test]
        [ExpectedException()]
        public void Execute_FailsUntilRetryCountExceeded_ThenGivesUp()
        {
            var expectedUri = "test://queue.abc/whatever/";
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri))
                                            .EnqueueResponse(new TimeoutException("message 1"))
                                            .EnqueueResponse(new TimeoutException("message 2"))
                                            .EnqueueResponse(new TimeoutException("message 3"))
                                            .EnqueueResponse(HttpStatusCode.OK, "Success", "Should give up before it gets this one", false);
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var request = new RequestWithEmptyPayload(new SettingsFake(), expectedUri, "GET");
            request.RetryPolicy = new RetryPolicy<ExceptionRetryStrategy>(2, TimeSpan.FromMilliseconds(1));

            request.Execute();

            // expecting an exception
        }

        //[Test]
        //public void Execute_FailsUntilRetryCountExceeded_ThrowsRetriedExceptionWithRetryCountInfo()
        //{
        //    // sorry, this one is a little bit of a mess because I wanted to verify an internal property on the exception
        //    try
        //    {
        //        // arrange
        //        var expectedUri = "test://queue.abc/whatever/";
        //        var expectedRequest = new TestableWebRequest(uri)
        //                                    .EnqueueResponse(new TimeoutException("message"))
        //                                    .EnqueueResponse(new TimeoutException("message"))
        //                                    .EnqueueResponse(new TimeoutException("message"));
        //        TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
        //        var request = new RequestWithEmptyPayload(new SettingsFake(), expectedUri, "GET");
        //        request.RetryPolicy = new RetryPolicy<ExceptionRetryStrategy>(2, TimeSpan.FromMilliseconds(1));

        //        // act
        //        var response = request.Execute();
        //    }
        //    catch (Exception exc)
        //    {
        //        //assert
        //        Assert.IsAssignableFrom<RetriedException>(exc);
        //        Assert.AreEqual(3, ((RetriedException)exc).RetryCount);

        //        return;
        //    }
        //    Assert.Fail("Did not receive expected exception");
        //}

        #endregion
    }


}
