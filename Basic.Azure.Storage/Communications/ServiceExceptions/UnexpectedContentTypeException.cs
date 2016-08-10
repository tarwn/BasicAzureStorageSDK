using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.ServiceExceptions
{
    public class UnexpectedContentTypeException : Exception
    {
        public UnexpectedContentTypeException(string contentType)
            : base ("Unexpected Content-Type received: " + contentType ?? "(null)")
        { }
    }
}
