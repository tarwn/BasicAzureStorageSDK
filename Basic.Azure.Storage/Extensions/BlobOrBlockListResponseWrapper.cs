using System;
using Basic.Azure.Storage.Communications.BlobService.BlobOperations;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Extensions.Contracts;

namespace Basic.Azure.Storage.Extensions
{
    public class BlobOrBlockListResponseWrapper : IBlobOrBlockListResponseWrapper
    {
        private readonly IBlobPropertiesResponse _response;
        
        public string ETag { get { return Response.ETag; } }
        public DateTime LastModified { get { return Response.LastModified; } }
        public DateTime Date { get { return Response.Date; } }
        public string ContentMD5 { get { return Response.ContentMD5; } }

        public IBlobPropertiesResponse Response
        {
            get
            {
                return _response;
            }
        }

        public bool IsPutBlobResponse { get; private set; }

        public bool IsPutBlockListResponse { get; private set; }

        public BlobOrBlockListResponseWrapper(IBlobPropertiesResponse response)
        {
            IsPutBlockListResponse = (response is PutBlockListResponse);
            IsPutBlobResponse = (response is PutBlobResponse);
            
            _response = response;
        }
    }
}
