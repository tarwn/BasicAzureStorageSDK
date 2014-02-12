using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    public class PutBlobRequest : RequestBase<EmptyResponsePayload>, 
                                  ISendAdditionalRequiredHeaders, 
                                  ISendAdditionalOptionalHeaders,
                                  ISendDataWithRequest
    {
        private string _containerName;
        private string _blobName;
        private BlobType _blobType;
        private byte[] _data;
        private string _contentType;
        private string _contentEncoding;
        private string _contentLanguage;

        public PutBlobRequest(StorageAccountSettings settings, string containerName, string blobName, BlobType blobType, byte[] data,
                    string contentType = null, string contentEncoding = null, string contentLanguage = null)
            : base(settings)
        {
            _containerName = containerName;
            _blobName = blobName;
            _blobType = blobType;
            _data = data;
            _contentType = contentType;
            _contentEncoding = contentEncoding;
            _contentLanguage = contentLanguage;
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
            if(_blobType == BlobType.Block)
                request.Headers.Add(ProtocolConstants.Headers.BlobType, ProtocolConstants.HeaderValues.BlobType.Block);
            else
                request.Headers.Add(ProtocolConstants.Headers.BlobType, ProtocolConstants.HeaderValues.BlobType.Page);
        }

        public void ApplyAdditionalOptionalHeaders(System.Net.WebRequest request)
        {
            if (!string.IsNullOrEmpty(_contentType))
                request.ContentType = _contentType;

            if (!string.IsNullOrEmpty(_contentEncoding))
                request.Headers.Add(ProtocolConstants.Headers.ContentEncoding, _contentEncoding);

            if (!string.IsNullOrEmpty(_contentLanguage))
                request.Headers.Add(ProtocolConstants.Headers.ContentLanguage, _contentLanguage);
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
