using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.ServiceExceptions
{
    public class AzureResponseParseException : Exception
    {
        public AzureResponseParseException(string field, string unexpectedValue)
            : base(String.Format("The field {0} has an unexpected value of '{1}'", field, unexpectedValue))
        { 
            
        }
    }
}
