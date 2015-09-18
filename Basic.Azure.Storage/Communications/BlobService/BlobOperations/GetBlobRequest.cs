using System;
using System.Net;
using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    /// <summary>
    ///     Get the blob
    ///     https://msdn.microsoft.com/en-us/library/azure/dd179440.aspx
    /// </summary>
    public class GetBlobRequest : RequestBase<GetBlobResponse>, ISendAdditionalOptionalHeaders
    {
        private readonly string _blobName;
        private readonly string _containerName;
        private readonly BlobRange _range;
        private readonly string _leaseId;

        public GetBlobRequest(StorageAccountSettings settings, string containerName, string blobName, BlobRange range, string leaseId = null)
            : base(settings)
        {
            Console.WriteLine("GetBlobRequest(settings {0}, containerName {1}, blobName {2}, range {3}, leaseId {4}", settings, containerName, blobName, range, leaseId);
            _containerName = containerName;
            _blobName = blobName;
            _range = range;

            if (null != leaseId) {
              Guard.ArgumentIsAGuid("leaseId", leaseId);
            }

            _leaseId = leaseId;
            Console.WriteLine("~GetBlobRequest()");
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
            if (_range != null)
                request.Headers.Add(ProtocolConstants.Headers.BlobRange, _range.GetStringValue());

            if (!string.IsNullOrEmpty(_leaseId))
                request.Headers.Add(ProtocolConstants.Headers.LeaseId, _leaseId);
        }
    }
}
