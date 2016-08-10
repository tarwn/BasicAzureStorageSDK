using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.TableService.TableOperations
{
    public class QueryTablesRequest : RequestBase<QueryTablesResponse>,
                                      ISendAdditionalRequiredHeaders
    {

        public QueryTablesRequest(StorageAccountSettings settings)
            : base(settings)
        { }

        protected override string HttpMethod { get { return "GET"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.TableService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.TableEndpoint);
            builder.AddSegment("Tables");
            return builder;
        }

        public void ApplyAdditionalRequiredHeaders(System.Net.WebRequest request)
        {
            if (request is HttpWebRequest)
            {
                ((HttpWebRequest)request).Accept = "application/json;odata=nometadata";
            }
            else
            {
                request.Headers.Add("Accept", "application/json;odata=nometadata");
            }
        }

    }
}
