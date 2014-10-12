using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.QueueService.MessageOperations
{
    /// <summary>
    /// Add a message to the specified Queue
    /// http://msdn.microsoft.com/en-us/library/azure/dd179474.aspx
    /// </summary>
    public class GetMessagesRequest : RequestBase<GetMessagesResponse>
    {
        private string _queueName;
        private int _numOfMessages;
        private int _visibilityTimeout;
        private int? _messageTtl;

        public GetMessagesRequest(StorageAccountSettings settings, string queueName, int numOfMessages = 1, int visibilityTimeout = 30, int? messageTtl = null)
            : base(settings)
        {
            //TODO: add Guard statements against invalid values, short circuit so we don't have the latency roundtrip to the server
            //TODO: add Guad statements for visibilityTimeout

            _queueName = queueName;
            _numOfMessages = numOfMessages;

            _visibilityTimeout = visibilityTimeout;
            _messageTtl = messageTtl;
        }

        protected override string HttpMethod { get { return "GET"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.QueueService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.QueueEndpoint);
            builder.AddSegment(_queueName);
            builder.AddSegment("messages");

            builder.AddParameter(ProtocolConstants.QueryParameters.NumOfMessages, _numOfMessages.ToString());

            builder.AddParameter(ProtocolConstants.QueryParameters.VisibilityTimeout, _visibilityTimeout.ToString());

            if (_messageTtl.HasValue)
                builder.AddParameter(ProtocolConstants.QueryParameters.MessageTTL, _messageTtl.Value.ToString());

            return builder;
        }

    }
}
