using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Basic.Azure.Storage.Communications.TableService.EntityOperations
{
    public class InsertOrReplaceEntityResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse
    {

        public string ETag { get; private set; }
        
        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            ETag = response.Headers[ProtocolConstants.Headers.ETag];

        }

    }
}
