using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.TableService.Interfaces;
using Basic.Azure.Storage.Communications.Utility;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.TableService.EntityOperations
{
    /// <summary>
    /// Queries entities in a table
    /// MSDN: https://msdn.microsoft.com/en-us/library/azure/dd179421.aspx
    /// </summary>
    public class QueryEntitiesRequest<TEntity> : RequestBase<QueryEntitiesResponse<TEntity>>,
                                                ISendAdditionalRequiredHeaders
        where TEntity : ITableEntity, new()
    {
        private string _tableName;
        private string _partitionKey;
        private string _rowKey;

        public QueryEntitiesRequest(StorageAccountSettings settings, string tableName, string partitionKey, string rowKey)
            : base(settings)
        {
            //Guard.IsValidTableName(tableName);
            //Guard.IsValidTablePartitionKey(partitionKey);
            //Guard.IsValidTableRowKey(rowKey);

            _tableName = tableName;
            _partitionKey = partitionKey;
            _rowKey = rowKey;
        }

        protected override string HttpMethod { get { return "GET"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.TableService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.TableEndpoint);
            if (!String.IsNullOrEmpty(_partitionKey) || !String.IsNullOrEmpty(_rowKey))
                builder.AddSegment(String.Format("{0}(PartitionKey='{1}',RowKey='{2}')", _tableName, _partitionKey, _rowKey));
            else
                builder.AddSegment(String.Format("{0}()", _tableName));
            return builder;
        }

        public void ApplyAdditionalRequiredHeaders(System.Net.WebRequest request)
        {
            request.ContentType = "application/json;charset=utf-8";

            if (request is HttpWebRequest)
            {
                ((HttpWebRequest)request).Accept = "application/json;odata=minimalmetadata";
            }
            else
            {
                request.Headers.Add(ProtocolConstants.Headers.Accept, "application/json;odata=minimalmetadata");
            }
        }

    }
}
