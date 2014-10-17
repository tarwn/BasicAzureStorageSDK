using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.ServiceExceptions
{
    public class AzureResponseParseError : Exception
    {
        public AzureResponseParseError(string message)
            : base(message)
        { 
            
        }
    }
}
