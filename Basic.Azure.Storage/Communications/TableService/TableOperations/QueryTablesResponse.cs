using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Basic.Azure.Storage.Communications.TableService.TableOperations
{
    public class QueryTablesResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse, IReceiveDataWithResponse
    {
        public string ContinuationNextTableName { get; private set; }

        public string ContentType { get; private set; }

        public List<string> TableList { get; private set; }

        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            if (response.Headers[ProtocolConstants.Headers.ContinuationNextTableName] != null)
            {
                ContinuationNextTableName = response.Headers[ProtocolConstants.Headers.ContinuationNextTableName];
            }

            if (response.Headers[ProtocolConstants.Headers.ContentType] != null)
            {
                ContentType = response.Headers[ProtocolConstants.Headers.ContentType];
            }
        }

        public async Task ParseResponseBodyAsync(Stream responseStream, string contentType)
        {
            using (StreamReader sr = new StreamReader(responseStream))
            {
                var content = await sr.ReadToEndAsync();

                if (ContentType.StartsWith("application/json;odata=nometadata") || ContentType.StartsWith("application/json;odata=minimalmetadata"))
                {
                    var tables = JObject.Parse(content);
                    TableList = tables["value"].Select(v => v["TableName"].Value<string>()).ToList();
                }
                else
                {
                    throw new UnexpectedContentTypeException(ContentType);
                }
            }
        }

    }
}
