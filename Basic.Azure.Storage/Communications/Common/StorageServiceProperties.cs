using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Common
{
    public class StorageServiceProperties
    {
        public StorageServiceProperties()
        { 
            // for 2012-02-12 and earlier all elements are required
            Logging = StorageServiceLoggingProperties.GetDefault();
            HourMetrics = StorageServiceMetricsProperties.GetDefault();
            MinuteMetrics = StorageServiceMetricsProperties.GetDefault();
            Cors = new List<StorageServiceCorsRule>();
        }

        public StorageServiceLoggingProperties Logging { get; set; }

        [Obsolete("Azure API 2013-08-15 split this into separate Hour and Minute results - this deprecated property returns the HourMetrics")]
        public StorageServiceMetricsProperties Metrics { get { return HourMetrics; } }

        public StorageServiceMetricsProperties HourMetrics { get; set; }

        public StorageServiceMetricsProperties MinuteMetrics { get; set; }

        public List<StorageServiceCorsRule> Cors { get; set; }
    }
}
