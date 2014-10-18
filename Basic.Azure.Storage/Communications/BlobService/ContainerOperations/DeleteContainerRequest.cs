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
    /// Deletes a container with the given name
    /// http://msdn.microsoft.com/en-us/library/azure/dd179408.aspx
    /// </summary>
    public class DeleteContainerRequest : RequestBase<EmptyResponsePayload>, ISendAdditionalOptionalHeaders
    {
        private string _containerName;
        private string _leaseId;

        public DeleteContainerRequest(StorageAccountSettings settings, string containerName, string leaseId = null)
            : base(settings)
        {
            _containerName = containerName;
            _leaseId = leaseId;
        }

        protected override string HttpMethod { get { return "DELETE"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.BlobService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.BlobEndpoint);
            builder.AddSegment(_containerName);
            builder.AddParameter(ProtocolConstants.QueryParameters.ResType, ProtocolConstants.QueryValues.ResType.Container);
            return builder;
        }

        public void ApplyAdditionalOptionalHeaders(System.Net.WebRequest request)
        {
            if (!string.IsNullOrEmpty(_leaseId))
            {
                request.Headers.Add(ProtocolConstants.Headers.LeaseId, _leaseId);
            }
        }
    }
}
