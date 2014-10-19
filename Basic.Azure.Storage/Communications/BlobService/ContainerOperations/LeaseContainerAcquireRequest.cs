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
    /// Lease Container - Only the Acquire Lease action
    /// http://msdn.microsoft.com/en-us/library/azure/jj159103.aspx
    /// </summary>
    /// <remarks>
    /// This is separated into multipe requests because they have different 
    /// behavior and responses depending on the Lease Action. I think very
    /// discrete requests and responses will be easier to work with, despite
    /// the slight change from the common pattern of 1 request per API call.
    /// </remarks>
    public class LeaseContainerAcquireRequest : RequestBase<LeaseContainerAcquireResponse>, ISendAdditionalOptionalHeaders
    {
        private string _containerName;
        private int _leaseDurationInSeconds;
        private string _proposedLeaseId;

        /// <param name="leaseDurationTimeInSeconds">-1 for infinite, otherwise 15 to 60 seconds is allowed</param>
        public LeaseContainerAcquireRequest(StorageAccountSettings settings, string containerName, int leaseDurationTimeInSeconds, string proposedLeaseId = null)
            : base(settings)
        {
            Guard.ArgumentIsNotNullOrEmpty("containerName", containerName);
            Guard.ArgumentInRanges("leaseDurationInSeconds", leaseDurationTimeInSeconds, new GuardRange<int>[] { new GuardRange<int>(-1), new GuardRange<int>(15, 60) });
            if (!string.IsNullOrEmpty(proposedLeaseId))
                Guard.ArgumentIsAGuid("proposedLeaseId", proposedLeaseId);

            _containerName = containerName;
            _leaseDurationInSeconds = leaseDurationTimeInSeconds;
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
            request.Headers.Add(ProtocolConstants.Headers.LeaseAction, ProtocolConstants.HeaderValues.LeaseAction.Acquire);
            request.Headers.Add(ProtocolConstants.Headers.LeaseDuration, _leaseDurationInSeconds.ToString());
            if (!string.IsNullOrEmpty(_proposedLeaseId))
            {
                request.Headers.Add(ProtocolConstants.Headers.ProposedLeaseId, _proposedLeaseId);
            }
        }
    }
}
