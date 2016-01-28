using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using System.Text;
using System.Xml.Linq;

namespace Basic.Azure.Storage.Communications.QueueService.MessageOperations
{
    /// <summary>
    /// Add a message to the specified Queue
    /// http://msdn.microsoft.com/en-us/library/windowsazure/dd179346.aspx
    /// </summary>
    public class PutMessageRequest : RequestBase<EmptyResponsePayload>, ISendDataWithRequest
    {
        private readonly string _queueName;
        private readonly int? _visibilityTimeout;
        private readonly int? _messageTtl;

        private readonly byte[] _content;

        public PutMessageRequest(StorageAccountSettings settings, string queueName, string messageData, int? visibilityTimeout = null, int? messageTtl = null)
            : base(settings)
        {
            //TODO: add Guard statements against invalid values, short circuit so we don't have the latency roundtrip to the server
            _queueName = queueName;
            _visibilityTimeout = visibilityTimeout;
            _messageTtl = messageTtl;

            _content = PrepareContent(messageData);
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
