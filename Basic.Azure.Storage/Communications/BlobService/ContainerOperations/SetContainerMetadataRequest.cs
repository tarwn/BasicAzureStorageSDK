using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.BlobService.ContainerOperations
{
    /// <summary>
    /// Adds the specified metadata to the container
    /// http://msdn.microsoft.com/en-us/library/azure/dd179362.aspx
    /// </summary>
    public class SetContainerMetadataRequest : RequestBase<EmptyResponsePayload>, ISendAdditionalOptionalHeaders
    {
        private string _containerName;
        private Dictionary<string, string> _metadata;
        private string _lease;

        public SetContainerMetadataRequest(StorageAccountSettings settings, string containerName, Dictionary<string,string> metadata, string lease = null)
            : base(settings)
        {
            _containerName = containerName;
            _metadata = metadata;
            _lease = lease;
        }

        protected override string HttpMethod { get { return "PUT"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.BlobService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.BlobEndpoint);
            builder.AddSegment(_containerName);
            builder.AddParameter(ProtocolConstants.QueryParameters.ResType, ProtocolConstants.QueryValues.ResType.Container);
            builder.AddParameter(ProtocolConstants.QueryParameters.Comp, ProtocolConstants.QueryValues.Metadata);
            return builder;
        }

        public void ApplyAdditionalOptionalHeaders(System.Net.WebRequest request)
        {
            if(!String.IsNullOrEmpty(_lease))
                request.Headers.Add(ProtocolConstants.Headers.LeaseId, _lease);

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
