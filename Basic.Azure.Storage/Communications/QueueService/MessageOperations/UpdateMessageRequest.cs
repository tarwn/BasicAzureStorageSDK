using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using System.Text;
using System.Xml.Linq;

namespace Basic.Azure.Storage.Communications.QueueService.MessageOperations
{
    /// <summary>
    /// Updates the visibility and, optionally, content of a message in the queue
    /// http://msdn.microsoft.com/en-us/library/azure/hh452234.aspx
    /// </summary>
    public class UpdateMessageRequest : RequestBase<UpdateMessageResponse>, ISendDataWithRequest
    {
        private readonly string _queueName;
        private readonly string _messageId;
        private readonly string _popReceipt;
        private readonly int _visibilityTimeout;

        private readonly byte[] _content;

        public UpdateMessageRequest(StorageAccountSettings settings, string queueName, string messageId, string popReceipt, int visibilityTimeout = 30, string messageData = null)
            : base(settings)
        {
            //TODO: add Guard statements against invalid values, short circuit so we don't have the latency roundtrip to the server
            _queueName = queueName;
            _messageId = messageId;
            _popReceipt = popReceipt;
            _visibilityTimeout = visibilityTimeout;

            _content = PrepareContent(messageData);
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

        private static byte[] PrepareContent(string content)
        {
            if (string.IsNullOrEmpty(content))
            {
                return new byte[] { };
            }

            var messageWithEnvelopeBuilder =
                new XElement("QueueMessage",
                    new XElement("MessageText", content));

            return Encoding.UTF8.GetBytes(messageWithEnvelopeBuilder.ToString(SaveOptions.DisableFormatting));
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
