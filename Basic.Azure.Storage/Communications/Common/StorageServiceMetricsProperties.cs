using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Basic.Azure.Storage.Communications.Common
{
    public class StorageServiceMetricsProperties
    {
        public StorageAnalyticsVersionNumber Version { get; set; }
        public bool Enabled { get; set; }
        public bool IncludeAPIs { get; set; }

        public bool RetentionPolicyEnabled { get; set; }
        public int RetentionPolicyNumberOfDays { get; set; }

        public static StorageServiceMetricsProperties GetDefault()
        {
            return new StorageServiceMetricsProperties()
            {
                Version = StorageAnalyticsVersionNumber.v1_0,
                Enabled = false,
                IncludeAPIs = false,
                RetentionPolicyEnabled = false,
                RetentionPolicyNumberOfDays = 0
            };
        }
    }
}
