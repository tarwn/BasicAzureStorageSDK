using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;
using System.Collections.Generic;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    /// <summary>
    ///     Put the blob (block or page)
    ///     https://msdn.microsoft.com/en-us/library/azure/dd179451.aspx
    /// </summary>
    public class CopyBlobRequest : RequestBase<CopyBlobResponse>,
                                  ISendAdditionalRequiredHeaders,
                                  ISendAdditionalOptionalHeaders
    {
        private readonly string _containerName;
        private readonly string _blobName;
        private readonly string _copySource;
        private readonly Dictionary<string, string> _metadata;
        private readonly string _leaseId;

        public CopyBlobRequest(StorageAccountSettings settings, string containerName, string blobName, string copySource, Dictionary<string, string> metadata = null, string leaseId = null)
            : base(settings)
        {
            Guard.ArgumentIsNotNullOrEmpty("containerName", containerName);
            Guard.ArgumentIsNotNullOrEmpty("blobName", blobName);
            Guard.ArgumentIsValidAbsoluteUri("copySource", copySource);
            if(!string.IsNullOrEmpty(leaseId))
                Guard.ArgumentIsAGuid("leaseId", leaseId);
            if(null != metadata)
                IdentifierValidation.EnsureNamesAreValidIdentifiers(metadata.Keys);

            _containerName = containerName;
            _blobName = blobName;
            _copySource = copySource;
            _metadata = metadata;
            _leaseId = leaseId;
        }

        protected override string HttpMethod { get { return "PUT"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.BlobService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.BlobEndpoint);
            builder.AddSegment(_containerName);
            builder.AddSegment(_blobName);
            return builder;
        }

        public void ApplyAdditionalRequiredHeaders(System.Net.WebRequest request)
        {
            request.Headers.Add(ProtocolConstants.Headers.CopySource, _copySource);
        }

        public void ApplyAdditionalOptionalHeaders(System.Net.WebRequest request)
        {
            if (!string.IsNullOrEmpty(_leaseId))
                request.Headers.Add(ProtocolConstants.Headers.LeaseId, _leaseId);

            MetadataParse.PrepareAndApplyMetadataHeaders(_metadata, request);
        }
    }
}
