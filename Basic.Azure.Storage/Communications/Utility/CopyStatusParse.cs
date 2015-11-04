using System;
using Basic.Azure.Storage.Communications.Common;

namespace Basic.Azure.Storage.Communications.Utility
{
    public static class CopyStatusParse
    {
        public const string Success = "success";
        public const string Pending = "pending";
        public const string Aborted = "aborted";
        public const string Failed = "failed";

        public static CopyStatus ParseCopyStatus(string copyStatus)
        {
            switch (copyStatus)
            {
                case Success:
                    return CopyStatus.Success;
                    break;
                case Pending:
                    return CopyStatus.Pending;
                    break;
                case Aborted:
                    return CopyStatus.Aborted;
                    break;
                case Failed:
                    return CopyStatus.Failed;
                    break;
                default:
                    throw new ArgumentException(String.Format("Provided copy status [{0}] cannot be parsed.", copyStatus), copyStatus);
                    break;
            }
        }
    }
}