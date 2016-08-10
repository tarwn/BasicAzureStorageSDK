using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.TableService.Interfaces;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.TableService.EntityOperations
{
    public class DeleteEntityRequest : RequestBase<EmptyResponsePayload>,
                                        ISendAdditionalRequiredHeaders
    {
        private string _tableName;
        private string _partitionKey;
        private string _rowKey;
        private string _etag;

        public DeleteEntityRequest(StorageAccountSettings settings, string tableName, string partitionKey, string rowKey, string etag)
            : base(settings)
        {
            _tableName = tableName;
            _partitionKey = partitionKey;
            _rowKey = rowKey;
            if (string.IsNullOrEmpty(etag))
                _etag = "*";
            else
                _etag = etag;
        }

        protected override string HttpMethod { get { return "DELETE"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.TableService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.TableEndpoint);
            builder.AddSegment(String.Format("{0}(PartitionKey='{1}',RowKey='{2}')", _tableName, _partitionKey, _rowKey));
            return builder;
        }

        public void ApplyAdditionalRequiredHeaders(System.Net.WebRequest request)
        {
            request.ContentType = "application/json;charset=utf-8";

            if (request is HttpWebRequest)
            {
                ((HttpWebRequest)request).Accept = "application/json";
            }
            else
            {
                request.Headers.Add(ProtocolConstants.Headers.Accept, "application/json");
            }

            request.Headers.Add(ProtocolConstants.Headers.IfMatch, _etag);
        }

    }
}
