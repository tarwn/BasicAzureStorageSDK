using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.QueueService.QueueOperations
{
    /// <summary>
    /// Set metadata for queue
    /// http://msdn.microsoft.com/en-us/library/azure/dd179348.aspx
    /// </summary>
    public class SetQueueMetadataRequest : RequestBase<EmptyResponsePayload>, ISendAdditionalOptionalHeaders
    {
        private string _queueName;
        private Dictionary<string, string> _metadata;

        public SetQueueMetadataRequest(StorageAccountSettings settings, string queueName, Dictionary<string,string> metadata)
            : base(settings)
        {
            _queueName = queueName;
            _metadata = metadata;
        }

        protected override string HttpMethod { get { return "PUT"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.QueueService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.QueueEndpoint);
            builder.AddSegment(_queueName);
            builder.AddParameter(ProtocolConstants.QueryParameters.Comp, ProtocolConstants.QueryValues.Metadata);
            return builder;
        }


        public void ApplyAdditionalOptionalHeaders(WebRequest request)
        {
            if (_metadata != null && _metadata.Count > 0)
            {
                foreach (var kvp in _metadata.Select(kvp => kvp))
                {
                    request.Headers.Add(String.Format("{0}{1}", ProtocolConstants.Headers.MetaDataPrefix, kvp.Key), kvp.Value);
                }
            }
        }
    }
}
