using Basic.Azure.Storage.Communications.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.QueueService
{
    public class CreateQueueRequest : RequestBase<EmptyResponsePayload>
    {
        private string _queueName;

        public CreateQueueRequest(StorageAccountSettings settings, string queueName)
            : base(settings)
        {
            _queueName = queueName;
        }

        protected override string HttpMethod { get { return "PUT"; } }

        protected override AuthenticationMethod AuthenticationMethod { get { return AuthenticationMethod.SharedKeyForBlobAndQueueServices; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.QueueEndpoint);
            builder.AddSegment(_queueName);
            return builder;
        }

        protected override void ApplyRequiredHeaders(WebRequest request)
        {

        }

        protected override void ApplyOptionalHeaders(WebRequest request)
        {

        }

    }
}
