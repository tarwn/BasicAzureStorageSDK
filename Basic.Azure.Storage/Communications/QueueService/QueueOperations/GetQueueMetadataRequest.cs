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
    /// Retrieve metadata for queue, including approximate message count
    /// http://msdn.microsoft.com/en-us/library/azure/dd179384.aspx
    /// </summary>
    public class GetQueueMetadataRequest : RequestBase<GetQueueMetadataResponse>
    {
        private string _queueName;

        public GetQueueMetadataRequest(StorageAccountSettings settings, string queueName)
            : base(settings)
        {
            _queueName = queueName;
        }

        protected override string HttpMethod { get { return "GET"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.QueueService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.QueueEndpoint);
            builder.AddSegment(_queueName);
            builder.AddParameter(ProtocolConstants.QueryParameters.Comp, ProtocolConstants.QueryValues.Metadata);
            return builder;
        }

    }
}
