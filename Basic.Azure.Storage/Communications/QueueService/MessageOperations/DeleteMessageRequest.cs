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
    /// Deletes the specified queue item from the queue
    /// http://msdn.microsoft.com/en-us/library/azure/dd179347.aspx
    /// </summary>
    public class DeleteMessageRequest : RequestBase<EmptyResponsePayload>
    {
        private string _queueName;
        private string _messageId;
        private string _popReceipt;

        public DeleteMessageRequest(StorageAccountSettings settings, string queueName, string messageId, string popReceipt)
            : base(settings)
        {
            //TODO: add Guard statements against invalid values, short circuit so we don't have the latency roundtrip to the server
            _queueName = queueName;
            _messageId = messageId;
            _popReceipt = popReceipt;
        }

        protected override string HttpMethod { get { return "DELETE"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.QueueService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.QueueEndpoint);
            builder.AddSegment(_queueName);
            builder.AddSegment("messages");
            builder.AddSegment(_messageId);

            builder.AddParameter(ProtocolConstants.QueryParameters.PopReceipt, _popReceipt);

            return builder;
        }

    }
}
