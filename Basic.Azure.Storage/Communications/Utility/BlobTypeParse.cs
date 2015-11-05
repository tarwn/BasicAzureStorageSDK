using System;
using Basic.Azure.Storage.Communications.Common;

namespace Basic.Azure.Storage.Communications.Utility
{
    public static class BlobTypeParse
    {
        private const string PageBlob = "PageBlob";
        private const string BlockBlob = "BlockBlob";
        private const string AppendBlob = "AppendBlob";

        public static BlobType ParseBlobType(string rawBlobType)
        {
            switch (rawBlobType)
            {
                case PageBlob:
                    return BlobType.Page;
                    break;
                case BlockBlob:
                    return BlobType.Block;
                    break;
                case AppendBlob:
                    throw new ArgumentException("Append blobs are not supported at this time.", "rawBlobType");
                default:
                    throw new ArgumentException(string.Format("The given raw blob type [{0} cannot be parsed.", rawBlobType), "rawBlobType");
            }
        }
    }
}