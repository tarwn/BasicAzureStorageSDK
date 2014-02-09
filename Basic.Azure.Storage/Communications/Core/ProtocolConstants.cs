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

            public const string Date = "x-ms-date";

            public const string MetaDataPrefix = "x-ms-meta-";

            public const string StorageVersion = "x-ms-version";

            public const string UserAgent = "User-Agent";
        }

    }
}
