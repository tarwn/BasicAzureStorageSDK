using System;
using System.Net;
using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.Core;

namespace Basic.Azure.Storage.Communications.Utility
{
    public static class CopyHeadersParse
    {
        public static DateTime? ParseCopyCompletionTime(WebResponse response)
        {
            var rawCompletionTime = response.Headers[ProtocolConstants.Headers.CopyCompletionTime];
            if (null == rawCompletionTime)
            {
                return null;
            }

            return DateParse.ParseHeader(rawCompletionTime);
        }

        public static string ParseCopyStatusDescription(WebResponse response)
        {
            return response.Headers[ProtocolConstants.Headers.CopyStatusDescription];
        }

        public static string ParseCopyId(WebResponse response)
        {
            return response.Headers[ProtocolConstants.Headers.CopyId];
        }

        public static BlobCopyProgress ParseCopyProgress(WebResponse response)
        {
            return CopyProgressParse.ParseCopyProgress(response.Headers[ProtocolConstants.Headers.CopyProgress]);
        }

        public static string ParseCopySource(WebResponse response)
        {
            return response.Headers[ProtocolConstants.Headers.CopySource];
        }

        public static CopyStatus? ParseCopyStatus(WebResponse response)
        {
            var rawCopyStatus = response.Headers[ProtocolConstants.Headers.CopyStatus];
            if (null == rawCopyStatus)
            {
                return null;
            }

            return CopyStatusParse.ParseCopyStatus(rawCopyStatus);
        }
    }
}