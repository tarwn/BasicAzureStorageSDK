using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using TestableHttpWebResponse.ResponseSettings;

namespace TestableHttpWebResponse
{
	public class TestableHttpWebResponse : HttpWebResponse
	{
		private Stream _responseStream;

		public TestableHttpWebResponse(SerializationInfo serializationInfo, StreamingContext streamingContext, Stream responseStream)
			#pragma warning disable 0618
			/* this base ctor is deprecated */
			: base(serializationInfo, streamingContext)
			#pragma warning restore 0618
		{
			_responseStream = responseStream;
		}

		public override System.IO.Stream GetResponseStream()
		{
			return _responseStream;
		}

        public static TestableHttpWebResponse GetHttpWebResponse(HttpResponseSettings httpResponseSettings, Uri uri, string expectedContentType)
        {
            SerializationInfo si = new SerializationInfo(typeof(HttpWebResponse), new System.Runtime.Serialization.FormatterConverter());
            StreamingContext sc = new StreamingContext();
            WebHeaderCollection headers = new WebHeaderCollection();

            foreach (var kvp in httpResponseSettings.HeaderValues)
                headers.Add(kvp.Key, kvp.Value);

            si.AddValue("m_HttpResponseHeaders", headers);
            si.AddValue("m_Uri", uri);
            si.AddValue("m_Certificate", null);
            si.AddValue("m_Version", HttpVersion.Version11);
            si.AddValue("m_StatusCode", httpResponseSettings.StatusCode);
            si.AddValue("m_ContentLength", 0);
            si.AddValue("m_Verb", "GET");
            si.AddValue("m_StatusDescription", httpResponseSettings.StatusDescription);
            si.AddValue("m_MediaType", expectedContentType);

            var webResponse = new TestableHttpWebResponse(si, sc, httpResponseSettings.ResponseStream);
 
            if (httpResponseSettings.ExpectException)
                throw new WebException("This request failed", new Exception(httpResponseSettings.StatusDescription), WebExceptionStatus.ProtocolError, webResponse);
            else
                return webResponse;
        }

    }
}
