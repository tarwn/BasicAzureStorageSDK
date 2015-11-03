using System;
using Basic.Azure.Storage.Communications.BlobService;

namespace Basic.Azure.Storage.Communications.Utility
{
    public static class CopyStatusParse
    {
        public const string Success = "success";
        public const string Pending = "pending";

        public static BlobCopyStatus ParseCopyStatus(string copyStatus)
        {
            switch (copyStatus)
            {
                case Success:
                    return BlobCopyStatus.Success;
                    break;
                case Pending:
                    return BlobCopyStatus.Pending;
                    break;
                default:
                    throw new ArgumentException(String.Format("Provided copy status [{0}] cannot be parsed.", copyStatus), copyStatus);
                    break;
            }
        }
    }
}