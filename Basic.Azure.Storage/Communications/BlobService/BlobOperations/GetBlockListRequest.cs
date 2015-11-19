using System.Net;
using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    /// <summary>
    /// Get the blob's metadata
    /// https://msdn.microsoft.com/en-us/library/azure/dd179350.aspx
    /// </summary>
    public class GetBlockListRequest : RequestBase<GetBlockListResponse>, ISendAdditionalOptionalHeaders
    {
        private readonly string _containerName;
        private readonly string _blobName;
        private readonly string _leaseId;
        private readonly GetBlockListListType _blockListType;

        public GetBlockListRequest(StorageAccountSettings settings, string containerName, string blobName, string leaseId = null, GetBlockListListType blockListType = GetBlockListListType.Committed)
            : base(settings)
        {
            Guard.ArgumentIsNotNullOrEmpty("containerName", containerName);
            Guard.ArgumentIsNotNullOrEmpty("blobName", blobName);
            if (!string.IsNullOrEmpty(leaseId))
                Guard.ArgumentIsAGuid("leaseId", leaseId);

            _containerName = containerName;
            _blobName = blobName;
            _leaseId = leaseId;
            _blockListType = blockListType;
        }

        protected override string HttpMethod { get { return "GET"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.BlobService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.BlobEndpoint);

            builder.AddSegment(_containerName);
            builder.AddSegment(_blobName);

            builder.AddParameter(ProtocolConstants.QueryParameters.Comp, ProtocolConstants.QueryValues.BlockList);
            builder.AddParameter(ProtocolConstants.QueryParameters.BlockListType, _blockListType.ToString().ToLower());

            return builder;
        }

        public void ApplyAdditionalOptionalHeaders(WebRequest request)
        {
            if (!string.IsNullOrEmpty(_leaseId))
                request.Headers.Add(ProtocolConstants.Headers.LeaseId, _leaseId);
        }
    }
}
