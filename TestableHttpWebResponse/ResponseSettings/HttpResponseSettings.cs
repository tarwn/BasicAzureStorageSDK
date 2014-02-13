using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

namespace TestableHttpWebResponse.ResponseSettings
{
	public class HttpResponseSettings : BaseResponseSettings
	{
        private string _responseContent;

		public HttpStatusCode StatusCode { get; set; }
		public string StatusDescription { get; set; }

        public Dictionary<string, string> HeaderValues { get; protected set; }

        public string ResponseContent
        {
            get { return _responseContent; }
            set
            {
                _responseContent = value;
                ResponseStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(_responseContent));
            }
        }
		public HttpResponseSettings(HttpStatusCode httpStatusCode, string statusDescription, string responseContent, bool expectWebExceptionToBeThrown, Dictionary<string,string> headerValues = null)
		{
			StatusCode = httpStatusCode;
			StatusDescription = statusDescription;
			ResponseContent = responseContent;
			ExpectException = expectWebExceptionToBeThrown;
            HeaderValues = headerValues ?? new Dictionary<string,string>();
		}


		public System.IO.Stream ResponseStream { get; set; }
	}
}
