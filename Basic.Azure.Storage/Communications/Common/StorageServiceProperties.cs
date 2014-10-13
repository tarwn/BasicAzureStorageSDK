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
            Metrics = StorageServiceMetricsProperties.GetDefault();
        }

        public StorageServiceLoggingProperties Logging { get; set; }

        public StorageServiceMetricsProperties Metrics { get; set; }
    }
}
