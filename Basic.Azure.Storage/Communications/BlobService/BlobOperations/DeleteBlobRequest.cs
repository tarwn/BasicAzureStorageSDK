using System.Net;
using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    /// <summary>
    /// Get the blob
    /// https://msdn.microsoft.com/en-us/library/azure/dd179440.aspx
    /// </summary>
    public class DeleteBlobRequest : RequestBase<DeleteBlobResponse>, ISendAdditionalOptionalHeaders
    {
        private readonly string _containerName;
        private readonly string _blobName;
        private readonly string _leaseId;

        public DeleteBlobRequest(StorageAccountSettings settings, string containerName, string blobName, string leaseId = null)
            : base(settings)
        {
            if (null != leaseId)
                Guard.ArgumentIsAGuid("leaseId", leaseId);

            _containerName = containerName;
            _blobName = blobName;
            _leaseId = leaseId;
        }

        protected override string HttpMethod { get { return "DELETE"; } }

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
            if (null != _leaseId)
                request.Headers.Add(ProtocolConstants.Headers.LeaseId, _leaseId);
        }

    }
}
