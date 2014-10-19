using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.BlobService.ContainerOperations
{
    /// <summary>
    /// Lease Container - Only the Release Lease action
    /// http://msdn.microsoft.com/en-us/library/azure/jj159103.aspx
    /// </summary>
    /// <remarks>
    /// This is separated into multiple requests because they have different 
    /// behavior and responses depending on the Lease Action. I think very
    /// discrete requests and responses will be easier to work with, despite
    /// the slight change from the common pattern of 1 request per API call.
    /// </remarks>
    public class LeaseContainerReleaseRequest : RequestBase<EmptyResponsePayload>, ISendAdditionalOptionalHeaders
    {
        private string _containerName;
        private string _leaseId;

        public LeaseContainerReleaseRequest(StorageAccountSettings settings, string containerName, string leaseId)
            : base(settings)
        {
            Guard.ArgumentIsNotNullOrEmpty("containerName", containerName);
            Guard.ArgumentIsAGuid("leaseId", leaseId);

            _containerName = containerName;
            _leaseId = leaseId;
        }

        protected override string HttpMethod { get { return "PUT"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.BlobService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.BlobEndpoint);
            builder.AddSegment(_containerName);
            builder.AddParameter(ProtocolConstants.QueryParameters.ResType, ProtocolConstants.QueryValues.ResType.Container);
            builder.AddParameter(ProtocolConstants.QueryParameters.Comp, ProtocolConstants.QueryValues.Comp.Lease);
            return builder;
        }

        public void ApplyAdditionalOptionalHeaders(System.Net.WebRequest request)
        {
            request.Headers.Add(ProtocolConstants.Headers.LeaseAction, ProtocolConstants.HeaderValues.LeaseAction.Release);
            request.Headers.Add(ProtocolConstants.Headers.LeaseId, _leaseId);
        }
    }
}
