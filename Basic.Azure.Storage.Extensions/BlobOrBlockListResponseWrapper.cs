using Basic.Azure.Storage.Communications.BlobService.BlobOperations;

namespace Basic.Azure.Storage.Extensions
{
    public class BlobOrBlockListResponseWrapper
    {
        private readonly PutBlockListResponse _putBlockListResponse;
        private readonly PutBlobResponse _putBlobResponse;

        public PutBlockListResponse PutBlockListResponse
        {
            get
            {
                return IsPutBlockListResponse 
                    ? _putBlockListResponse 
                    : null;
            }
        }

        public PutBlobResponse PutBlobResponse
        {
            get
            {
                return IsPutBlobResponse
                    ? _putBlobResponse
                    : null;
            }
        }

        public bool IsPutBlobResponse { get; set; }

        public bool IsPutBlockListResponse { get; set; }

        public BlobOrBlockListResponseWrapper(PutBlockListResponse response)
        {
            IsPutBlockListResponse = true;
            IsPutBlobResponse = false;

            _putBlockListResponse = response;
        }

        public BlobOrBlockListResponseWrapper(PutBlobResponse response)
        {
            IsPutBlockListResponse = false;
            IsPutBlobResponse = true;

            _putBlobResponse = response;
        }
    }
}
