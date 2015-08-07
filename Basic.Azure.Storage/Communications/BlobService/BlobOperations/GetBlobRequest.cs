using System;
using Basic.Azure.Storage.Communications.Core;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    /// <summary>
    /// Get the blob
    /// https://msdn.microsoft.com/en-us/library/azure/dd179440.aspx
    /// </summary>
    public class GetBlobRequest : RequestBase<GetBlobResponse>, ISendAdditionalOptionalHeaders
    {
        private string _containerName;
        private string _blobName;
        private BlobRange _range;

        public GetBlobRequest(StorageAccountSettings settings, string containerName, string blobName, BlobRange range)
            : base(settings)
        {
            _containerName = containerName;
            _blobName = blobName;
            _range = range;
        }

        protected override string HttpMethod { get { return "GET"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.BlobService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.BlobEndpoint);
            builder.AddSegment(_containerName);
            builder.AddSegment(_blobName);

            if (_snapshot.HasValue)
                builder.AddParameter(
                    ProtocolConstants.QueryParameters.Snapshot,
                    _snapshot.Value.ToString(ProtocolConstants.Parsing.DateTimeToStringFormat));

            if (_timeout.HasValue)
                builder.AddParameter(ProtocolConstants.QueryParameters.Timeout, _timeout.Value.ToString());

            return builder;
        }

        public void ApplyAdditionalOptionalHeaders(System.Net.WebRequest request)
        {
            if (_range != null)
                request.Headers.Add(ProtocolConstants.Headers.BlobRange, _range.GetStringValue());

        }
    }
}
