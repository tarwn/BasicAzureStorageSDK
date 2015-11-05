using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.Core;

namespace Basic.Azure.Storage.Communications.Utility
{
    public static class Parsers
    {
        private const string CopiedGroupName = "copied";
        private const string TotalGroupName = "total";
        private const string RegexMatch = @"^(?<" + CopiedGroupName + @">\d*)\/(?<" + TotalGroupName + @">\d*)$";
        private static readonly Regex Matcher = new Regex(RegexMatch);
        
        private const string PageBlob = "PageBlob";
        private const string BlockBlob = "BlockBlob";
        private const string AppendBlob = "AppendBlob";
        
        public const string Success = "success";
        public const string Pending = "pending";
        public const string Aborted = "aborted";
        public const string Failed = "failed";

        public static DateTime? ParseCopyCompletionTime(WebResponse response)
        {
            var rawCompletionTime = response.Headers[ProtocolConstants.Headers.CopyCompletionTime];
            if (null == rawCompletionTime)
            {
                return null;
            }

            return ParseDateHeader(rawCompletionTime);
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
            return ParseCopyProgress(response.Headers[ProtocolConstants.Headers.CopyProgress]);
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

            return ParseCopyStatus(rawCopyStatus);
        }

        public static BlobCopyProgress ParseCopyProgress(string copyProgress)
        {
            if (null == copyProgress)
                return null;

            var match = Matcher.Match(copyProgress);

            if (!match.Success)
                return null;

            var copied = Int32.Parse(match.Groups[CopiedGroupName].Value);
            var total = Int32.Parse(match.Groups[TotalGroupName].Value);
            return new BlobCopyProgress(copied, total);
        }
        
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
                    throw new ArgumentException(String.Format("The given raw blob type [{0} cannot be parsed.", rawBlobType), "rawBlobType");
            }
        }

        public static DateTime ParseUTCDate(string dateIn8601)
        {
            // Per Jon Skeet: http://stackoverflow.com/questions/10029099/datetime-parse2012-09-30t230000-0000000z-always-converts-to-datetimekind-l
            return DateTime.ParseExact(dateIn8601,
                "yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'",
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
        }

        public static DateTime ParseDateHeader(string headerValue)
        {
            DateTime dateValue;
            DateTime.TryParse(headerValue, out dateValue);
            return dateValue;
        }

        public static ReadOnlyDictionary<string, string> ParseMetadata(WebResponse response)
        {
            var parsedMetadata = response.Headers.AllKeys
                .Where(key => key.StartsWith(ProtocolConstants.Headers.MetaDataPrefix, StringComparison.InvariantCultureIgnoreCase))
                .ToDictionary(key => key.Substring(ProtocolConstants.Headers.MetaDataPrefix.Length), key => response.Headers[key]);

            return new ReadOnlyDictionary<string, string>(parsedMetadata);
        }

        public static void PrepareAndApplyMetadataHeaders(IDictionary<string, string> givenMetadata, WebRequest request)
        {
            const string joinParts = ProtocolConstants.Headers.MetaDataPrefix + "{0}";

            if (givenMetadata == null || givenMetadata.Count == 0) return;
            
            foreach (var kvp in givenMetadata)
            {
                request.Headers.Add(String.Format(joinParts, kvp.Key), kvp.Value);
            }
        }
    }
}