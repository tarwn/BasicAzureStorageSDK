using Basic.Azure.Storage.Communications;
using Basic.Azure.Storage.Communications.Core;
using Microsoft.Practices.TransientFaultHandling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Tests.Fakes
{
    public class RequestWithEmptyPayload : RequestBase<EmptyResponsePayload>
    {

        public RequestWithEmptyPayload(StorageAccountSettings settings, string uri, string httpMethod)
            : base(settings)
        {
            TestRequestUri = uri;
            TestHttpMethod = httpMethod;
            // use a faster retry policy with the same strategy as the real one
            RetryPolicy = new RetryPolicy<ExceptionRetryStrategy>(3, TimeSpan.FromMilliseconds(1));
        }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(TestRequestUri);
            return builder;
        }

        protected override string HttpMethod { get { return TestHttpMethod; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.QueueService; } }

        protected override void ApplyRequiredHeaders(WebRequest request)
        { }

        protected override void ApplyOptionalHeaders(WebRequest request)
        { }

        public string TestHttpMethod { get; protected set; }
        public string TestRequestUri { get; protected set; }
    }

}
