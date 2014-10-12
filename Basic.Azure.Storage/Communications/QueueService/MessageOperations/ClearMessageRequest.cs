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
    public class ClearMessageRequest : RequestBase<EmptyResponsePayload>
    {
        private string _queueName;

        public ClearMessageRequest(StorageAccountSettings settings, string queueName)
            : base(settings)
        {
            //TODO: add Guard statements against invalid values, short circuit so we don't have the latency roundtrip to the server
            _queueName = queueName;
        }

        protected override string HttpMethod { get { return "DELETE"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.QueueService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.QueueEndpoint);
            builder.AddSegment(_queueName);
            builder.AddSegment("messages");

            return builder;
        }

    }
}
