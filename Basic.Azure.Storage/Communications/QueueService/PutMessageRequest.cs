using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.QueueService
{
    public class PutMessageRequest : RequestBase<EmptyResponsePayload>, ISendDataWithRequest
    {
        private string _queueName;
        private string _messageData;

        public PutMessageRequest(StorageAccountSettings settings, string queueName, string messageData)
            : base(settings)
        {
            _queueName = queueName;
            _messageData = messageData;
        }

        protected override string HttpMethod { get { return "POST"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.QueueService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.QueueEndpoint);
            builder.AddSegment(_queueName);
            builder.AddSegment("messages");
            // TODO: add querystring options for timeout, messagettl, visibilitytimeout
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
