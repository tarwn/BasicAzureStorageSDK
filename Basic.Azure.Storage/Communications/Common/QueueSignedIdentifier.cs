using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Common
{
    public class QueueSignedIdentifier
    {
        public string Id { get; set; }

        public QueueAccessPolicy AccessPolicy { get; set; }
    }
}
