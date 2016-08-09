using Basic.Azure.Storage.Communications.TableService;

namespace Basic.Azure.Storage.Communications.Core
{
    public static class ProtocolConstants
    {
        public static class Headers
        {
            public const string ApproximateMessagesCount = "x-ms-approximate-messages-count";
            public const string Authorization = "Authorization";
            public const string BlobCacheControl = "x-ms-blob-cache-control";
            public const string BlobContentEncoding = "x-ms-blob-content-encoding";
            public const string BlobContentLanguage = "x-ms-blob-content-language";
            public const string BlobContentLength = "x-ms-blob-content-length";
            public const string BlobContentType = "x-ms-blob-content-type";
            public const string BlobContentMD5 = "x-ms-blob-content-md5";
            public const string BlobPublicAccess = "x-ms-blob-public-access";
            public const string BlobRange = "x-ms-range";
            public const string BlobSequenceNumber = "x-ms-blob-sequence-number";
            public const string BlobType = "x-ms-blob-type";
            public const string CacheControl = "Cache-Control";
            public const string ContentEncoding = "Content-Encoding";
            public const string ContentLanguage = "Content-Language";
            public const string ContentLength = "Content-Length";
            public const string ContentMD5 = "Content-MD5";
            public const string ContentType = "Content-Type";
            public const string ContinuationNextTableName = "x-ms-continuation-NextTableName";
            public const string CopyCompletionTime = "x-ms-copy-completion-time";
            public const string CopyId = "x-ms-copy-id";
            public const string CopyProgress = "x-ms-copy-progress";
            public const string CopySource = "x-ms-copy-source";
            public const string CopyStatus = "x-ms-copy-status";
            public const string CopyStatusDescription = "x-ms-copy-status-description";
            public const string Date = "x-ms-date";
            public const string ETag = "ETag";
            public const string LastModified = "Last-Modified";
            public const string LeaseAction = "x-ms-lease-action";
            public const string LeaseBreakPeriod = "x-ms-lease-break-period";
            public const string LeaseDuration = "x-ms-lease-duration";
            public const string LeaseId = "x-ms-lease-id";
            public const string LeaseState = "x-ms-lease-state";
            public const string LeaseStatus = "x-ms-lease-status";
            public const string MetaDataPrefix = "x-ms-meta-";
            public const string OperationDate = "Date";
            public const string ProposedLeaseId = "x-ms-proposed-lease-id";
            public const string PopReceipt = "x-ms-popreceipt";
            public const string PreferenceApplied = "Preference-Applied";
            public const string RequestId = "x-ms-request-id";
            public const string StorageVersion = "x-ms-version";
            public const string UserAgent = "User-Agent";
            public const string Version = "x-ms-version";
        }

        public static class HeaderValues
        {
            public static class BlobPublicAccess
            {
                public const string Container = "container";
                public const string Blob = "blob";
            }

            public static class BlobType
            {
                public const string Block = "BlockBlob";
                public const string Page = "PageBlob";
            }

            public class LeaseAction
            {
                public const string Acquire = "acquire";
                public const string Renew = "renew";
                public const string Change = "change";
                public const string Release = "release";
                public const string Break = "break";
            }

            public static class LeaseDuration
            {
                public const string Fixed = "fixed";
                public const string Infinite = "infinite";
            }

            public static class LeaseState
            {
                public const string Available = "available";
                public const string Leased = "leased";
                public const string Expired = "expired";
                public const string Breaking = "breaking";
                public const string Broken = "broken";
            }

            public static class LeaseStatus
            {
                public const string Locked = "locked";
                public const string Unlocked = "unlocked";
            }

            public static class TableMetadataPreference
            {
                public const string ReturnContent = "return-content";
                public const string ReturnNoContent = "return-no-content";

                public static string GetValue(MetadataPreference value)
                {
                    switch (value)
                    {
                        case MetadataPreference.ReturnContent:
                            return ReturnContent;
                        case MetadataPreference.ReturnNoContent:
                        default:
                            return ReturnNoContent;
                    }
                }
            }
        }

        public static class QueryParameters
        {
            public const string BlockId = "blockid";
            public const string BlockListType = "blocklisttype";
            public const string Comp = "comp";
            public const string Delimiter = "delimiter";
            public const string Include = "include";
            public const string Marker = "marker";
            public const string MaxResults = "maxresults";
            public const string MessageTTL = "messagettl";
            public const string NumOfMessages = "numofmessages";
            public const string PopReceipt = "popreceipt";
            public const string Prefix = "prefix";
            public const string ResType = "restype";
            public const string Timeout = "timeout";
            public const string VisibilityTimeout = "visibilitytimeout";
        }

        public static class QueryValues
        {
            public const string ACL = "acl";
            public const string BlockList = "blocklist";
            public const string List = "list";
            public const string Metadata = "metadata";

            public static class Comp
            {
                public const string Lease = "lease";
                public const string Properties = "properties";
            }

            public static class ResType
            {
                public const string Container = "container";
                public const string Service = "service";
            }
        }
    }
}
