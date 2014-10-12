using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.ServiceExceptions
{
    public class UnrecognizedAzureException : AzureException
    {
        public UnrecognizedAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, Dictionary<string,string> details, WebException baseException)
            : base(requestId, statusCode, String.Format("Unrecognized error '{0}'", statusDescription), details, baseException) { }
    }
}
