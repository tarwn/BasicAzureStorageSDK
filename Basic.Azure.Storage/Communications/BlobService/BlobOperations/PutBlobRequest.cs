using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;
using System.Collections.Generic;
using Basic.Azure.Storage.Communications.Common;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    /// <summary>
    ///     Put the blob (block or page)
    ///     https://msdn.microsoft.com/en-us/library/azure/dd179451.aspx
    /// </summary>
    public class PutBlobRequest : RequestBase<PutBlobResponse>,
                                  ISendAdditionalRequiredHeaders,
                                  ISendAdditionalOptionalHeaders,
                                  ISendDataWithRequest
    {
        private readonly string _containerName;
        private readonly string _blobName;
        private readonly BlobType _blobType;
        private readonly byte[] _data;
        private readonly int _pageContentLength;
        private readonly string _contentType;
        private readonly string _contentEncoding;
        private readonly string _contentLanguage;
        private readonly string _contentMD5;
        private readonly string _cacheControl;
        private readonly Dictionary<string, string> _metadata;
        private readonly long _sequenceNumber = 0;
        private readonly string _leaseId;

        /// <summary>
        /// BlockBlob Type
        /// </summary>
        public PutBlobRequest(StorageAccountSettings settings, string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null, string leaseId = null)
            : this(settings, containerName, blobName, BlobType.Block, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata, leaseId)
        {
            Guard.ArgumentArrayLengthIsEqualOrSmallerThanSize("data", data, BlobServiceConstants.MaxSingleBlobUploadSize);

            _data = data;
        }

        /// <summary>
        /// PageBlob Type
        /// </summary>
        public PutBlobRequest(StorageAccountSettings settings, string containerName, string blobName, int pageContentLength,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null, long sequenceNumber = 0, string leaseId = null)
            : this(settings, containerName, blobName, BlobType.Page, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata, leaseId)
        {
            _pageContentLength = pageContentLength;
            _sequenceNumber = sequenceNumber;
        }

        private PutBlobRequest(StorageAccountSettings settings, string containerName, string blobName, BlobType blobType,
                    string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
                    string cacheControl = null, Dictionary<string, string> metadata = null, string leaseId = null)
            : base(settings)
        {
            if (!string.IsNullOrEmpty(leaseId))
                Guard.ArgumentIsAGuid("leaseId", leaseId);
            if (null != metadata)
                IdentifierValidation.EnsureNamesAreValidIdentifiers(metadata.Keys);

            _containerName = containerName;
            _blobName = blobName;
            _blobType = blobType;
            _contentType = contentType;
            _contentEncoding = contentEncoding;
            _contentLanguage = contentLanguage;
            _contentMD5 = contentMD5;
            _cacheControl = cacheControl;
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
            if (_blobType == BlobType.Block)
            {
                request.Headers.Add(ProtocolConstants.Headers.BlobType, ProtocolConstants.HeaderValues.BlobType.Block);
            }
            else
            {
                request.Headers.Add(ProtocolConstants.Headers.BlobType, ProtocolConstants.HeaderValues.BlobType.Page);
                request.Headers.Add(ProtocolConstants.Headers.BlobContentLength, _pageContentLength.ToString());
            }
        }

        public void ApplyAdditionalOptionalHeaders(System.Net.WebRequest request)
        {
            if (!string.IsNullOrEmpty(_contentType))
                request.ContentType = _contentType;

            if (!string.IsNullOrEmpty(_contentEncoding))
                request.Headers.Add(ProtocolConstants.Headers.ContentEncoding, _contentEncoding);

            if (!string.IsNullOrEmpty(_contentLanguage))
                request.Headers.Add(ProtocolConstants.Headers.ContentLanguage, _contentLanguage);

            if (!string.IsNullOrEmpty(_contentMD5))
                request.Headers.Add(ProtocolConstants.Headers.ContentMD5, _contentMD5);

            if (!string.IsNullOrEmpty(_cacheControl))
                request.Headers.Add(ProtocolConstants.Headers.CacheControl, _cacheControl);

            if (!string.IsNullOrEmpty(_leaseId))
                request.Headers.Add(ProtocolConstants.Headers.LeaseId, _leaseId);

            Parsers.PrepareAndApplyMetadataHeaders(_metadata, request);

            if (_blobType == BlobType.Page)
                request.Headers.Add(ProtocolConstants.Headers.BlobSequenceNumber, _sequenceNumber.ToString());
        }

        public byte[] GetContentToSend()
        {
            if (_blobType == BlobType.Block)
                return _data;
            else
                return new byte[] { };
        }

        public int GetContentLength()
        {
            if (_blobType == BlobType.Block)
                return _data.Length;
            else
                return 0;
        }
    }
}
