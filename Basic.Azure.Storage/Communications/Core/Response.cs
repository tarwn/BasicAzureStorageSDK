using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Core
{
    public class Response<T> : IDisposable
        where T : IResponsePayload, new()
    {
        private T _payload;
        private Stream _responseStreamToDisposeOf;

        public Response(HttpWebResponse httpWebResponse)
        {
            _payload = new T();
            HttpStatus = httpWebResponse.StatusCode;
            HttpStatusDescription = httpWebResponse.StatusDescription;

            ParseHeaders(httpWebResponse);
        }

        // seperated from constructor because I don't like doing network calls in ctor and 
        // we want to move disposal of stream later in lifecycle so we can optionally expose
        // the stream directly to caller
        public async Task ProcessResponseStreamAsync(HttpWebResponse httpWebResponse)
        {
            _responseStreamToDisposeOf = httpWebResponse.GetResponseStream();
            if (ExpectsResponseBody)
                await ((IReceiveDataWithResponse)_payload).ParseResponseBodyAsync(_responseStreamToDisposeOf);
            else
                await ReadResponseToNull(_responseStreamToDisposeOf);
        }

        public int NumberOfAttempts { get; set; }

        public HttpStatusCode HttpStatus { get; private set; }

        public string HttpStatusDescription { get; private set; }

        public string RequestId { get; private set; }

        public T Payload { get { return _payload; } }

        private bool ExpectsResponseBody
        {
            get
            {
                return typeof(IReceiveDataWithResponse).IsAssignableFrom(typeof(T));
            }
        }

        private bool ExpectsResponseHeaders
        {
            get
            {
                return typeof(IReceiveAdditionalHeadersWithResponse).IsAssignableFrom(typeof(T));
            }
        }

        private async Task ReadResponseToNull(Stream stream)
        {
            using (var sr = new StreamReader(stream))
            {
                await sr.ReadToEndAsync();
            }
        }

        private void ParseHeaders(HttpWebResponse response)
        { 
            //TODO: parse request id
            RequestId = "Not implemented";

            if (ExpectsResponseHeaders)
                ((IReceiveAdditionalHeadersWithResponse)_payload).ParseHeaders(response);
        }
        
        public void Dispose()
        {
            _responseStreamToDisposeOf.Dispose();
        }
    }
}
