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
    /// Retrieves one or more messages from the front of the queue without altering visibility of those messages
    /// http://msdn.microsoft.com/en-us/library/azure/dd179472.aspx
    /// </summary>
    public class PeekMessagesRequest : RequestBase<PeekMessagesResponse>
    {
        private string _queueName;
        private int _numOfMessages;
        private int? _messageTtl;

        public PeekMessagesRequest(StorageAccountSettings settings, string queueName, int numOfMessages = 1, int? messageTtl = null)
            : base(settings)
        {
            //TODO: add Guard statements against invalid values, short circuit so we don't have the latency roundtrip to the server
            
            _queueName = queueName;
            _numOfMessages = numOfMessages;

            _messageTtl = messageTtl;
        }

        protected override string HttpMethod { get { return "GET"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.QueueService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.QueueEndpoint);
            builder.AddSegment(_queueName);
            builder.AddSegment("messages");

            builder.AddParameter("peekonly", true.ToString());

            builder.AddParameter(ProtocolConstants.QueryParameters.NumOfMessages, _numOfMessages.ToString());

            if (_messageTtl.HasValue)
                builder.AddParameter(ProtocolConstants.QueryParameters.MessageTTL, _messageTtl.Value.ToString());

            return builder;
        }

    }
}
