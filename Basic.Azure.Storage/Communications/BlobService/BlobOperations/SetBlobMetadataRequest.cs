using System.Collections.Generic;
using System.Linq;
using System.Net;
using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    /// <summary>
    /// Set the blob's metadata
    /// https://msdn.microsoft.com/en-us/library/azure/dd179414.aspx
    /// </summary>
    public class SetBlobMetadataRequest : RequestBase<SetBlobMetadataResponse>,
                                        ISendAdditionalOptionalHeaders,
                                        ISendAdditionalRequiredHeaders
    {
        private readonly string _containerName;
        private readonly string _blobName;
        private readonly Dictionary<string, string> _metadata;
        private readonly string _leaseId;

        public SetBlobMetadataRequest(StorageAccountSettings settings, string containerName, string blobName, Dictionary<string, string> metadata, string leaseId = null)
            : base(settings)
        {
            Guard.ArgumentIsNotNullOrEmpty("containerName", containerName);
            Guard.ArgumentIsNotNullOrEmpty("blobName", blobName);
            Guard.ArgumentIsNotNull("metadata", metadata);
            IdentifierValidation.EnsureNamesAreValidIdentifiers(metadata.Keys);
            if (!string.IsNullOrEmpty(leaseId))
                Guard.ArgumentIsAGuid("leaseId", leaseId);

            _containerName = containerName;
            _blobName = blobName;
            _leaseId = leaseId;
            _metadata = metadata;
        }

        protected override string HttpMethod { get { return "PUT"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.BlobService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.BlobEndpoint);
            builder.AddSegment(_containerName);
            builder.AddSegment(_blobName);

            builder.AddParameter(ProtocolConstants.QueryParameters.Comp, ProtocolConstants.QueryValues.Metadata);

            return builder;
        }

        public void ApplyAdditionalRequiredHeaders(WebRequest request)
        {
            MetadataParse.PrepareAndApplyMetadataHeaders(_metadata, request);
        }

        public void ApplyAdditionalOptionalHeaders(WebRequest request)
        {
            if (!string.IsNullOrEmpty(_leaseId))
                request.Headers.Add(ProtocolConstants.Headers.LeaseId, _leaseId);
        }
    }
}
