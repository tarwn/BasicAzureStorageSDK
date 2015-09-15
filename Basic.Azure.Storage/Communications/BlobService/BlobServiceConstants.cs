namespace Basic.Azure.Storage.Communications.BlobService
{
    public static class BlobServiceConstants
    {
        private const int Megabye = 1024 * 1024;
        public const int MaxSingleBlobUploadSize = 64 * Megabye;
        public const int MaxSingleBlockUploadSize = 4 * Megabye; 
    }
}
