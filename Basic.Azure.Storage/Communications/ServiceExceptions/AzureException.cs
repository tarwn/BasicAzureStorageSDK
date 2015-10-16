using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace Basic.Azure.Storage.Communications.ServiceExceptions
{
    public class AzureException : Exception
    {
        public string RequestId { get; private set; }
        public HttpStatusCode StatusCode { get; private set; }
        public string StatusDescription { get; private set; }
        public Dictionary<string, string> Details { get; private set; }
        public Stack<Exception> ExceptionRetryStack { get; internal set; }
        public int RetryCount { get; internal set; }

        public AzureException(string requestId, HttpStatusCode statusCode, string statusDescription, Dictionary<string, string> details, WebException baseException, Stack<Exception> exceptionRetryStrack = null)
            : base(String.Format("{0}. Request {1}, Http Status: {2}\n{3}", statusDescription, requestId, (int)statusCode, string.Join(", ", details.Select(kvp => String.Format("{0}: {1}", kvp.Key, kvp.Value)))), baseException)
        {
            RequestId = requestId;
            StatusCode = statusCode;
            StatusDescription = statusDescription;
            Details = details;
            ExceptionRetryStack = exceptionRetryStrack ?? new Stack<Exception>();
            RetryCount = ExceptionRetryStack.Count;
        }

        /// <summary>
        /// This constructor is used only to wrap around other Azure Exceptionsd when Microsoft returns the wrong error code and we need to correct it for them
        /// </summary>
        public AzureException(AzureException actualExceptionToWrap)
            : base(actualExceptionToWrap.Message, actualExceptionToWrap)
        {
            RequestId = actualExceptionToWrap.RequestId;
            StatusCode = actualExceptionToWrap.StatusCode;
            StatusDescription = actualExceptionToWrap.StatusDescription;
            Details = actualExceptionToWrap.Details;
        }

    }
}
