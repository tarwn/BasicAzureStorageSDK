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
            Status = httpWebResponse.StatusCode;

            var responseStream = httpWebResponse.GetResponseStream();
            if (_payload.ExpectsResponseBody)
                _payload.ParseResponseBody(responseStream);
            else
                ReadResponseToNull(responseStream);
        }

        public HttpStatusCode Status { get; private set; }

        public int NumberOfAttempts { get; set; }

        private void ReadResponseToNull(Stream stream)
        {
            using (var sr = new StreamReader(stream))
            {
                sr.ReadToEnd();
            }
        }


    }
}
