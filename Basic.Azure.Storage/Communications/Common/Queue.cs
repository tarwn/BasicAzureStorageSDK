using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Common
{
    public class Queue
    {
        public Queue()
        {
            Metadata = new Dictionary<string, string>();
        }

        public string Name { get; set; }

        public Dictionary<string, string> Metadata { get; set; }
    }
}
