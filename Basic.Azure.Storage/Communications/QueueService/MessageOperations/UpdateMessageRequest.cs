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
    /// Updates the visibility and, optionally, content of a message in the queue
    /// http://msdn.microsoft.com/en-us/library/azure/hh452234.aspx
    /// </summary>
    public class UpdateMessageRequest : RequestBase<UpdateMessageResponse>, ISendDataWithRequest
    {
        private string _queueName;
        private string _messageId;
        private string _popReceipt;
        private int _visibilityTimeout;
        private string _messageData;

        private byte[] _content;

        public UpdateMessageRequest(StorageAccountSettings settings, string queueName, string messageId, string popReceipt, int visibilityTimeout = 30, string messageData = null)
            : base(settings)
        {
            //TODO: add Guard statements against invalid values, short circuit so we don't have the latency roundtrip to the server
            _queueName = queueName;
            _messageId = messageId;
            _popReceipt = popReceipt;
            _visibilityTimeout = visibilityTimeout;

            _messageData = messageData;

            PrepareContent();
        }

        protected override string HttpMethod { get { return "PUT"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.QueueService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.QueueEndpoint);
            builder.AddSegment(_queueName);
            builder.AddSegment("messages");
            builder.AddSegment(_messageId);

            builder.AddParameter(ProtocolConstants.QueryParameters.PopReceipt, _popReceipt);
            builder.AddParameter(ProtocolConstants.QueryParameters.VisibilityTimeout, _visibilityTimeout.ToString());

            return builder;
        }

        private void PrepareContent()
        {
            if (string.IsNullOrEmpty(_messageData))
            {
                _content = new byte[] { };
            }
            else
            {
                string messageWithEnvelope = String.Format("<QueueMessage><MessageText>{0}</MessageText></QueueMessage>", _messageData);
                _content = UTF8Encoding.UTF8.GetBytes(messageWithEnvelope);
            }
        }

        public byte[] GetContentToSend()
        {
            return _content;
        }

        public int GetContentLength()
        {
            return _content.Length;
        }
    }
}
