using System.Net;
using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;

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

        public GetBlobRequest(StorageAccountSettings settings, string containerName, string blobName, BlobRange range)
            : base(settings)
        {
            _containerName = containerName;
            _blobName = blobName;
            _range = range;
        }

        protected override string HttpMethod
        {
            get { return "GET"; }
        }

        protected override StorageServiceType ServiceType
        {
            get { return StorageServiceType.BlobService; }
        }

        public void ApplyAdditionalOptionalHeaders(WebRequest request)
        {
            if (_range != null)
                request.Headers.Add(ProtocolConstants.Headers.BlobRange, _range.GetStringValue());
        }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.BlobEndpoint);
            builder.AddSegment(_containerName);
            builder.AddSegment(_blobName);

            return builder;
        }
    }
}