using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.QueueService
{
    /// <summary>
    /// Add a message to the specified Queue
    /// http://msdn.microsoft.com/en-us/library/windowsazure/dd179346.aspx
    /// </summary>
    public class PutMessageRequest : RequestBase<EmptyResponsePayload>, ISendDataWithRequest
    {
        private string _queueName;
        private string _messageData;
        private int? _visibilityTimeout;
        private int? _messageTtl;

        public PutMessageRequest(StorageAccountSettings settings, string queueName, string messageData, int? visibilityTimeout = null, int? messageTtl = null)
            : base(settings)
        {
            //TODO: add Guard statements against invalid values, short circuit so we don't have the latency roundtrip to the server
            _queueName = queueName;
            _messageData = messageData;

            _visibilityTimeout = visibilityTimeout;
            _messageTtl = messageTtl;
        }

        protected override string HttpMethod { get { return "POST"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.QueueService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.QueueEndpoint);
            builder.AddSegment(_queueName);
            builder.AddSegment("messages");

            if (_visibilityTimeout.HasValue)
                builder.AddParameter(ProtocolConstants.QueryParameters.VisibilityTimeout, _visibilityTimeout.Value.ToString());

            if (_messageTtl.HasValue)
                builder.AddParameter(ProtocolConstants.QueryParameters.MessageTTL, _messageTtl.Value.ToString());

            return builder;
        }

        public byte[] GetContentToSend()
        {
            string messageWithEnvelope = String.Format("<QueueMessage><MessageText>{0}</MessageText></QueueMessage>", _messageData);
            return UTF8Encoding.UTF8.GetBytes(messageWithEnvelope);
        }

        public int GetContentLength()
        {
            return GetContentToSend().Length;
        }
    }
}
