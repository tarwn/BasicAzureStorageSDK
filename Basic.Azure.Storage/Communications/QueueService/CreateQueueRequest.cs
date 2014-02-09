using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.QueueService
{
    /// <summary>
    /// Create a queue with the given name
    /// http://msdn.microsoft.com/en-us/library/windowsazure/dd179342.aspx
    /// </summary>
    /// <remarks>
    /// Per the documentation, this is more of a "Create if not exists" operation
    /// </remarks>
    public class CreateQueueRequest : RequestBase<EmptyResponsePayload>
    {
        private string _queueName;
        private Dictionary<string, string> _metadata;

        public CreateQueueRequest(StorageAccountSettings settings, string queueName, Dictionary<string,string> metadata = null)
            : base(settings)
        {
            _queueName = queueName;
            _metadata = metadata;

            if(_metadata != null)
                IdentifierValidation.EnsureNamesAreValidIdentifiers(_metadata.Select(kvp => kvp.Key));
        }

        protected override string HttpMethod { get { return "PUT"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.QueueService; } }

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
            if (_metadata != null && _metadata.Count > 0)
            {
                foreach (var kvp in _metadata.Select(kvp => kvp))
                {
                    request.Headers.Add(String.Format("{0}{1}", ProtocolConstants.Headers.MetaDataPrefix, kvp.Key), kvp.Value);
                }
            }
        }

    }
}
