using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Basic.Azure.Storage.Communications.Common
{
    public class AccessPolicy
    {
        public DateTime StartTime { get; set; }
        public DateTime Expiry { get; set; }
        public string Permission { get; set; }
    }
}
