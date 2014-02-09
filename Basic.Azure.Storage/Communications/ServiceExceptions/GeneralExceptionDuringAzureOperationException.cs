using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.ServiceExceptions
{
    public class GeneralExceptionDuringAzureOperationException : Exception
    {
        public GeneralExceptionDuringAzureOperationException(string message, Exception innerException)
            : base(message, innerException)
        { }
    }
}
