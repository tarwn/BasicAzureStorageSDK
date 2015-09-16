using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    /// <summary>
    ///     Put the block list
    ///     https://msdn.microsoft.com/en-us/library/azure/dd179467.aspx
    /// </summary>
    public class PutBlockListRequest : RequestBase<PutBlockListResponse>,
                                  ISendAdditionalOptionalHeaders,
                                  ISendDataWithRequest
    {
        private readonly string _containerName;
        private readonly string _blobName;
        private readonly byte[] _data;
        private readonly string _requestContentMD5;
        private readonly string _cacheControl;
        private readonly string _contentType;
        private readonly string _contentEncoding;
        private readonly string _contentLanguage;
        private readonly string _blobContentMD5;
        private readonly Dictionary<string, string> _metadata;
        private readonly string _leaseId;


        /// <summary>
        /// BlockBlob Type
        /// </summary>
        public PutBlockListRequest(StorageAccountSettings settings, string containerName, string blobName, BlockListBlockIdList data,
            string cacheControl = null, string contentType = null,
            string contentEncoding = null, string contentLanguage = null, string blobContentMD5 = null,
            Dictionary<string, string> metadata = null, string leaseId = null)
            : base(settings)
        {
            if (null != leaseId)
                Guard.ArgumentIsAGuid("leaseId", leaseId);

            var dataAndHash = data.AsXmlByteArrayWithMd5Hash();
            _data = dataAndHash.XmlBytes;
            _requestContentMD5 = dataAndHash.MD5Hash;

            _containerName = containerName;
            _blobName = blobName;
            _contentType = contentType;
            _contentEncoding = contentEncoding;
            _contentLanguage = contentLanguage;
            _blobContentMD5 = blobContentMD5;
            _cacheControl = cacheControl;
            _metadata = metadata;
            _leaseId = leaseId;

            if (_metadata != null)
                IdentifierValidation.EnsureNamesAreValidIdentifiers(_metadata.Select(kvp => kvp.Key));
        }

        protected override string HttpMethod { get { return "PUT"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.BlobService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.BlobEndpoint);
            builder.AddSegment(_containerName);
            builder.AddSegment(_blobName);

            builder.AddParameter(ProtocolConstants.QueryParameters.Comp, "blocklist");

            return builder;
        }

        public void ApplyAdditionalOptionalHeaders(System.Net.WebRequest request)
        {
            if (!string.IsNullOrEmpty(_contentType))
                request.Headers.Add(ProtocolConstants.Headers.BlobContentType, _contentType);

            if (!string.IsNullOrEmpty(_contentEncoding))
                request.Headers.Add(ProtocolConstants.Headers.BlobContentEncoding, _contentEncoding);

            if (!string.IsNullOrEmpty(_contentLanguage))
                request.Headers.Add(ProtocolConstants.Headers.BlobContentLanguage, _contentLanguage);

            if (!string.IsNullOrEmpty(_requestContentMD5))
                request.Headers.Add(ProtocolConstants.Headers.ContentMD5, _requestContentMD5);

            if (!string.IsNullOrEmpty(_blobContentMD5))
                request.Headers.Add(ProtocolConstants.Headers.BlobContentMD5, _blobContentMD5);

            if (!string.IsNullOrEmpty(_cacheControl))
                request.Headers.Add(ProtocolConstants.Headers.BlobCacheControl, _cacheControl);

            if(!string.IsNullOrEmpty(_leaseId))
                request.Headers.Add(ProtocolConstants.Headers.LeaseId, _leaseId);

            if (_metadata != null && _metadata.Count > 0)
            {
                foreach (var kvp in _metadata.Select(kvp => kvp))
                {
                    request.Headers.Add(String.Format("{0}{1}", ProtocolConstants.Headers.MetaDataPrefix, kvp.Key), kvp.Value);
                }
            }
        }

        public byte[] GetContentToSend()
        {
            return _data;
        }

        public int GetContentLength()
        {
            return _data.Length;
        }
    }
}
