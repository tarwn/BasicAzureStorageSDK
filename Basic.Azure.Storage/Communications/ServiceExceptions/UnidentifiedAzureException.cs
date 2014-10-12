using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.ServiceExceptions
{
    public class UnidentifiedAzureException : AzureException
    {
        public UnidentifiedAzureException(WebException baseException)
            : base("No HTTP Response", HttpStatusCode.Unused, "Unknown Azure Error, no HttpWebResponse available", new Dictionary<string,string>(), baseException) { }

    }
}
