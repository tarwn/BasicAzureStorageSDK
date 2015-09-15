using System;
using System.Collections.Generic;
using System.Net;
using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Basic.Azure.Storage.Communications.Utility;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    public class PutBlockRequest : RequestBase<PutBlockResponse>,
                                  ISendAdditionalOptionalHeaders,
                                  ISendDataWithRequest
    {
        private readonly string _containerName;
        private readonly string _blobName;
        private readonly string _blockId;
        private readonly byte[] _data;
        private readonly string _contentMD5;
        private readonly string _leaseId;

        public PutBlockRequest(StorageAccountSettings settings, string containerName, string blobName, string blockId, byte[] data, string contentMD5 = null, string leaseId = null)
            : base(settings)
        {
            Guard.ArgumentIsBase64Encoded("blockId", blockId);
            Guard.ArgumentArrayLengthIsEqualOrSmallerThanSize("blockId", blockId.ToCharArray(), 64);

            Guard.ArgumentArrayLengthIsEqualOrSmallerThanSize("data", data, BlobServiceConstants.MaxSingleBlockUploadSize);

            if (null != leaseId)
                Guard.ArgumentIsAGuid("leaseId", leaseId);

            _containerName = containerName;
            _blobName = blobName;
            _blockId = blockId;
            _data = data;
            _contentMD5 = contentMD5;
            _leaseId = leaseId;
        }

        protected override string HttpMethod { get { return "PUT"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.BlobService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.BlobEndpoint);
            builder.AddSegment(_containerName);
            builder.AddSegment(_blobName);

            builder.AddParameter(ProtocolConstants.QueryParameters.Comp, "block");
            builder.AddParameter(ProtocolConstants.QueryParameters.BlockId, _blockId);

            return builder;
        }

        public void ApplyAdditionalOptionalHeaders(System.Net.WebRequest request)
        {
            if (!string.IsNullOrEmpty(_contentMD5))
                request.Headers.Add(ProtocolConstants.Headers.ContentMD5, _contentMD5);

            if (!string.IsNullOrEmpty(_leaseId))
                request.Headers.Add(ProtocolConstants.Headers.LeaseId, _leaseId);
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
