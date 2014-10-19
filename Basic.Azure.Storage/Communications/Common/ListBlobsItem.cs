using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Common
{
    public class ListBlobsItem
    {
        public ListBlobsItem()
        {
            Properties = new ListBlobsItemProperties();
            Metadata = new Dictionary<string, string>();
        }

        public string Name { get; set; }

        public DateTime? Snapshot { get; set; }

        public ListBlobsItemProperties Properties { get; set; }

        public Dictionary<string, string> Metadata { get; set; }
    }
}
