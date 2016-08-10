using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Common
{
    public class StorageServiceCorsRule
    {
        public StorageServiceCorsRule()
        {
            AllowedMethods = new List<string>();
            AllowedOrigins = new List<string>();
            ExposedHeaders = new List<string>();
            AllowedHeaders = new List<string>();
        }

        public List<string> AllowedOrigins { get; set; }
        public List<string> AllowedMethods { get; set; }
        public int MaxAgeInSeconds { get; set; }
        public List<string> ExposedHeaders { get; set; }
        public List<string> AllowedHeaders { get; set; }
    }

}
