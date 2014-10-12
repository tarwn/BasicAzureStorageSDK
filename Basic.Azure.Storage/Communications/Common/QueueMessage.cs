using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Common
{
    public class QueueMessage
    {
        public string MessageId { get; set; }
        public DateTime InsertionTime { get; set; }
        public DateTime ExpirationTime { get; set; }
        public string PopReceipt { get; set; }
        public DateTime TimeNextVisible { get; set; }
        public int DequeueCount { get; set; }
        public string MessageText { get; set; }

    }
}
