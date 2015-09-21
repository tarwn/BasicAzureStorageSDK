using System.Net;
using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    /// <summary>
    /// Get the blob's properties, including lease status and metadata
    /// https://msdn.microsoft.com/en-us/library/azure/dd179394.aspx
    /// </summary>
    public class GetBlobPropertiesRequest : RequestBase<GetBlobPropertiesResponse>, ISendAdditionalOptionalHeaders
    {
        private string _containerName;
        private string _blobName;
        private string _leaseId;

        public GetBlobPropertiesRequest(StorageAccountSettings settings, string containerName, string blobName, string leaseId = null)
            : base(settings)
        {
            Guard.ArgumentIsNotNullOrEmpty("containerName", containerName);
            Guard.ArgumentIsNotNullOrEmpty("blobName", blobName);

            _containerName = containerName;
            _blobName = blobName;
            _leaseId = leaseId;
        }

        protected override string HttpMethod { get { return "GET"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.BlobService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.BlobEndpoint);
            builder.AddSegment(_containerName);
            builder.AddSegment(_blobName);
            return builder;
        }

        public void ApplyAdditionalOptionalHeaders(WebRequest request)
        {
            if (!string.IsNullOrEmpty(_leaseId))
                request.Headers.Add(ProtocolConstants.Headers.LeaseId, _leaseId);
        }
    }
}
