using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.Serialization;
using System.Text;
using TestableHttpWebResponse.ResponseSettings;

namespace TestableHttpWebResponse
{
    /// <summary>
    /// A fake HttpWebRequest that allows us to set one or more responses to receive in order and contains some basic
    /// settable properties we need in a WebRequest
    /// </summary>
    /// <remarks>Based heavily on samples from here: http://blog.salamandersoft.co.uk/index.php/2009/10/how-to-mock-httpwebrequest-when-unit-testing/ </remarks>
    public class TestableWebRequest : WebRequest
    {
        private Uri _uri;
        private MemoryStream _requestStream;
        private ICredentials _credentials;
        private WebHeaderCollection _headers;
        private bool _preAuthenticate;

        private Queue<BaseResponseSettings> _expectedResponses;

        public TestableWebRequest(Uri uri)
        {
            _uri = uri;
            _expectedResponses = new Queue<BaseResponseSettings>();
            _headers = new WebHeaderCollection();
            Method = "GET";
        }

        public TestableWebRequest EnqueueResponse(HttpStatusCode httpStatusCode, string statusDescription, string responseContent, bool expectWebExceptionToBeThrown)
        {
            _expectedResponses.Enqueue(new HttpResponseSettings(httpStatusCode, statusDescription, responseContent, expectWebExceptionToBeThrown)
            {
                ResponseStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(responseContent))
            });
            return this;
        }

        public TestableWebRequest EnqueueResponse(Exception exception)
        {
            _expectedResponses.Enqueue(new ExceptionResponseSettings(exception));
            return this;
        }

        public TestableWebRequest EnqueueResponse(HttpResponseSettings response)
        {
            _expectedResponses.Enqueue(response);
            return this;
        }


        #region Overrides for WebRequest

        public override long ContentLength { get; set; }

        public override string ContentType { get; set; }

        public string DefaultExpectedContentType { get; set; }

        public override ICredentials Credentials
        {
            get
            {
                return _credentials;
            }
            set
            {
                _credentials = value;
            }
        }

        public override WebHeaderCollection Headers
        {
            get
            {
                return _headers;
            }
            set
            {
                _headers = value;
            }
        }

        public override string Method { get; set; }

        public override bool PreAuthenticate
        {
            get
            {
                return _preAuthenticate;
            }
            set
            {
                _preAuthenticate = value;
            }
        }

        public override Uri RequestUri { get { return _uri; } }

        public override Stream GetRequestStream()
        {
            _requestStream = new MemoryStream();
            return _requestStream;
        }

        public override IAsyncResult BeginGetRequestStream(AsyncCallback callback, object state)
        {
            var function = new Func<object>(() => { return null; });
            return function.BeginInvoke(callback, state);
        }

        public override Stream EndGetRequestStream(IAsyncResult asyncResult)
        {
            return GetRequestStream();
        }

        /// <summary>
        /// Builds an HttpWebResponse using a deprecated constructor and the next queued ResponseSettings for this request
        /// </summary>
        /// <remarks>This is based on the sample code here: http://stackoverflow.com/questions/87200/mocking-webresponses-from-a-webrequest </remarks>
        public override WebResponse GetResponse()
        {
            var responseSettings = _expectedResponses.Dequeue();

            if (responseSettings is HttpResponseSettings)
            {
                var httpResponseSettings = (HttpResponseSettings)responseSettings;
                var webResponse = TestableHttpWebResponse.GetHttpWebResponse(httpResponseSettings, _uri, DefaultExpectedContentType);

                if (httpResponseSettings.ExpectException)
                    throw new WebException("This request failed", new Exception(httpResponseSettings.StatusDescription), WebExceptionStatus.ProtocolError, webResponse);
                else
                    return webResponse;
            }
            else if (responseSettings is ExceptionResponseSettings)
            {
                throw ((ExceptionResponseSettings)responseSettings).ExceptionToThrow;
            }
            else
            {
                throw new ArgumentException(String.Format("No logic to handle a ResponseSettings object of type '{0}'", responseSettings.GetType().Name));
            }
        }

        public override IAsyncResult BeginGetResponse(AsyncCallback callback, object state)
        {
            var function = new Func<object>(() => { return null; });
            return function.BeginInvoke(callback, state);
        }

        public override WebResponse EndGetResponse(IAsyncResult asyncResult)
        {
            return GetResponse();
        }

        #endregion

        /// <summary>
        /// Returns the contents written to the Request stream
        /// </summary>
        public byte[] GetContent()
        {
            return _requestStream.ToArray();
        }

    }
}
