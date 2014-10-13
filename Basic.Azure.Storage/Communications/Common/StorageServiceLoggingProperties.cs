using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Basic.Azure.Storage.Communications.Common
{
    public class StorageServiceLoggingProperties
    {
        public StorageAnalyticsVersionNumber Version { get; set; }
        public bool Delete { get; set; }
        public bool Read { get; set; }
        public bool Write { get; set; }

        public bool RetentionPolicyEnabled { get; set; }
        public int RetentionPolicyNumberOfDays { get; set; }

        public static StorageServiceLoggingProperties GetDefault()
        {
            return new StorageServiceLoggingProperties()
            {
                Version = StorageAnalyticsVersionNumber.v1_0,
                Delete = false,
                Read = false,
                Write = false,
                RetentionPolicyEnabled = false,
                RetentionPolicyNumberOfDays = 1
            };
        }
    }
}
