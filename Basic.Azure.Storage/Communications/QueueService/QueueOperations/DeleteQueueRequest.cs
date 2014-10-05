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
    /// Delete a queue with the given name
    /// http://msdn.microsoft.com/en-us/library/azure/dd179436.aspx
    /// </summary>
    public class DeleteQueueRequest : RequestBase<EmptyResponsePayload>
    {
        private string _queueName;
        private Dictionary<string, string> _metadata;

        public DeleteQueueRequest(StorageAccountSettings settings, string queueName)
            : base(settings)
        {
            _queueName = queueName;
        }

        protected override string HttpMethod { get { return "DELETE"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.QueueService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.QueueEndpoint);
            builder.AddSegment(_queueName);
            return builder;
        }

    }
}
