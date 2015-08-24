using Basic.Azure.Storage.Communications.Core;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    /// <summary>
    /// Get the blob
    /// https://msdn.microsoft.com/en-us/library/azure/dd179440.aspx
    /// </summary>
    public class DeleteBlobRequest : RequestBase<DeleteBlobResponse>
    {
        private readonly string _containerName;
        private readonly string _blobName;

        public DeleteBlobRequest(StorageAccountSettings settings, string containerName, string blobName)
            : base(settings)
        {
            _containerName = containerName;
            _blobName = blobName;
        }

        protected override string HttpMethod { get { return "DELETE"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.BlobService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.BlobEndpoint);

            builder.AddSegment(_containerName);
            builder.AddSegment(_blobName);

            return builder;
        }


    }
}
