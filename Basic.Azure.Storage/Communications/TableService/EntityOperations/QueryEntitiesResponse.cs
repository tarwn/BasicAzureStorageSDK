using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Basic.Azure.Storage.Communications.TableService.Interfaces;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Basic.Azure.Storage.Communications.TableService.EntityOperations
{
    public class QueryEntitiesResponse<TEntity> : IResponsePayload, 
                                                  IReceiveAdditionalHeadersWithResponse,
                                                  IReceiveDataWithResponse
        where TEntity : ITableEntity, new()
    {

        public string ContinuationNextPartitionKey { get; private set; }
        public string ContinuationNextRowKey { get; private set; }
        public List<TEntity> Entities { get; private set; }

        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            ContinuationNextPartitionKey = response.Headers[ProtocolConstants.Headers.ContinuationNextPartitionKey];
            ContinuationNextRowKey = response.Headers[ProtocolConstants.Headers.ContinuationNextRowKey];
        }
        
        public async Task ParseResponseBodyAsync(Stream responseStream, string contentType)
        {

                using (StreamReader sr = new StreamReader(responseStream))
                {
                    var content = await sr.ReadToEndAsync();

                    if (contentType.StartsWith("application/json;odata=nometadata") || contentType.StartsWith("application/json;odata=minimalmetadata"))
                    {
                        // undocumented feature - single entity requests have a different response pattern than queries
                        var parsedContent = JObject.Parse(content);

                        if (IsResponseForSingleElement(parsedContent))
                        {
                            // flat response
                            Entities = new List<TEntity>();
                            Entities.Add(parsedContent.ToObject<TEntity>());
                        }
                        else
                        {
                            // response with value array
                            var values = parsedContent.SelectToken("value");
                            if (values != null)
                            {
                                Entities = values.Select(v => v.ToObject<TEntity>()).ToList();
                            }
                            else
                            {
                                Entities = new List<TEntity>();
                            }
                        }
                    }
                    else
                    {
                        throw new UnexpectedContentTypeException(contentType);
                    }
                }

        }

        private bool IsResponseForSingleElement(JObject response)
        {
            return response.Value<string>("odata.metadata").EndsWith("@Element");
        }

    }
}
