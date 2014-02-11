using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Core
{
    public class Response<T>
        where T : IResponsePayload, new()
    {
        private T _payload;

        public Response(HttpWebResponse httpWebResponse)
        {
            _payload = new T();
            HttpStatus = httpWebResponse.StatusCode;
            HttpStatusDescription = httpWebResponse.StatusDescription;

            ParseHeaders(httpWebResponse);

            var responseStream = httpWebResponse.GetResponseStream();
            if (ExpectsResponseBody)
                ((IReceiveDataWithResponse)_payload).ParseResponseBody(responseStream);
            else
                ReadResponseToNull(responseStream);
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

        private void ReadResponseToNull(Stream stream)
        {
            using (var sr = new StreamReader(stream))
            {
                sr.ReadToEnd();
            }
        }

        private void ParseHeaders(HttpWebResponse response)
        { 
            //TODO: parse request id
            RequestId = "Not implemented";

            if (ExpectsResponseHeaders)
                ((IReceiveAdditionalHeadersWithResponse)_payload).ParseHeaders(response);
        }

    }
}
