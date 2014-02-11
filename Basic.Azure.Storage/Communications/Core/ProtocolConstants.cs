using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Core
{
    public static class ProtocolConstants
    {

        public static class Headers
        {
            public const string Authorization = "Authorization";

            public const string BlobPublicAccess = "x-ms-blob-public-access";

            public const string Date = "x-ms-date";

            public const string MetaDataPrefix = "x-ms-meta-";

            public const string StorageVersion = "x-ms-version";

            public const string UserAgent = "User-Agent";
        }

        public static class HeaderValues
        {
            
            public static class BlobPublicAccess
            {

                public const string Container = "container";

                public const string Blob = "blob";

            }
        }

        public static class QueryParameters
        {
            public const string MessageTTL = "messagettl";

            public const string ResType = "restype";

            public const string Timeout = "timeout";

            public const string VisibilityTimeout = "visibilitytimeout";
        }

        public static class QueryValues
        {

            public static class ResType
            {

                public const string Container = "container";

            }

        }
    }
}
