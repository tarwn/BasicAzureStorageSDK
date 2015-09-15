using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    public class PutBlobRequest : RequestBase<PutBlobResponse>,
                                  ISendAdditionalRequiredHeaders,
                                  ISendAdditionalOptionalHeaders,
                                  ISendDataWithRequest
    {
        private string _containerName;
        private string _blobName;
        private BlobType _blobType;
        private byte[] _data;
        private int _pageContentLength;
        private string _contentType;
        private string _contentEncoding;
        private string _contentLanguage;
        private string _contentMD5;
        private string _cacheControl;
        private Dictionary<string, string> _metadata;
        private long _sequenceNumber = 0;

        /// <summary>
        /// BlockBlob Type
        /// </summary>
        public PutBlobRequest(StorageAccountSettings settings, string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string,string> metadata = null)
            : this(settings, containerName, blobName, BlobType.Block, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata)
        {
            Guard.ArgumentArrayLengthIsEqualOrSmallerThanSize("data", data, BlobServiceConstants.MaxSingleBlobUploadSize);

            _data = data;
        }

        /// <summary>
        /// PageBlob Type
        /// </summary>
        public PutBlobRequest(StorageAccountSettings settings, string containerName, string blobName, int pageContentLength,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null, long sequenceNumber = 0)
            : this(settings, containerName, blobName, BlobType.Page, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata)
        {
            _pageContentLength = pageContentLength;
            _sequenceNumber = sequenceNumber;
        }

        private PutBlobRequest(StorageAccountSettings settings, string containerName, string blobName, BlobType blobType,
                    string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
                    string cacheControl = null, Dictionary<string, string> metadata = null)
            : base(settings)
        {
            _containerName = containerName;
            _blobName = blobName;
            _blobType = blobType;
            _contentType = contentType;
            _contentEncoding = contentEncoding;
            _contentLanguage = contentLanguage;
            _contentMD5 = contentMD5;
            _cacheControl = cacheControl;
            _metadata = metadata;

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

            if (_metadata != null && _metadata.Count > 0)
            {
                foreach (var kvp in _metadata.Select(kvp => kvp))
                {
                    request.Headers.Add(String.Format("{0}{1}", ProtocolConstants.Headers.MetaDataPrefix, kvp.Key), kvp.Value);
                }
            }

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
