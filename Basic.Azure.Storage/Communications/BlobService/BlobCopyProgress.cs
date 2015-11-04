namespace Basic.Azure.Storage.Communications.BlobService
{
    public class BlobCopyProgress
    {
        public int BytesCopied { get; protected set; }
        public int BytesTotal { get; protected set; }

        public double PercentComplete { get { return (double)BytesCopied/BytesTotal; } }

        public BlobCopyProgress(int bytesCopied, int bytesTotal)
        {
            BytesCopied = bytesCopied;
            BytesTotal = bytesTotal;
        }
    }
}