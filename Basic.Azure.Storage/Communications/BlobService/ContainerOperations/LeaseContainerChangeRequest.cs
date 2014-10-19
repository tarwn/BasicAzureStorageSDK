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
    /// Lease Container - Only the Change Lease action
    /// http://msdn.microsoft.com/en-us/library/azure/jj159103.aspx
    /// </summary>
    /// <remarks>
    /// This is separated into multipe requests because they have different 
    /// behavior and responses depending on the Lease Action. I think very
    /// discrete requests and responses will be easier to work with, despite
    /// the slight change from the common pattern of 1 request per API call.
    /// </remarks>
    public class LeaseContainerChangeRequest : RequestBase<LeaseContainerChangeResponse>, ISendAdditionalOptionalHeaders
    {
        private string _containerName;
        private string _currentLeaseId;
        private string _proposedLeaseId;

        /// <param name="leaseDurationTimeInSeconds">-1 for infinite, otherwise 15 to 60 seconds is allowed</param>
        public LeaseContainerChangeRequest(StorageAccountSettings settings, string containerName, string currentLeaseId, string proposedLeaseId)
            : base(settings)
        {
            Guard.ArgumentIsNotNullOrEmpty("containerName", containerName);
            Guard.ArgumentIsAGuid("currentLeaseId", currentLeaseId);
            Guard.ArgumentIsAGuid("proposedLeaseId", proposedLeaseId);

            _containerName = containerName;
            _currentLeaseId = currentLeaseId;
            _proposedLeaseId = proposedLeaseId;
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
            request.Headers.Add(ProtocolConstants.Headers.LeaseAction, ProtocolConstants.HeaderValues.LeaseAction.Change);
            request.Headers.Add(ProtocolConstants.Headers.LeaseId, _currentLeaseId);
            request.Headers.Add(ProtocolConstants.Headers.ProposedLeaseId, _proposedLeaseId);
        }
    }
}
