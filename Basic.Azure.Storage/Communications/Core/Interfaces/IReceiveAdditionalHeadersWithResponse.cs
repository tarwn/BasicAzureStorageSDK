using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Core.Interfaces
{
    public interface IReceiveAdditionalHeadersWithResponse
    {
        void ParseHeaders(HttpWebResponse response);
    }
}
