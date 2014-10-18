using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Common
{
    public class BlobSignedIdentifier
    {
        public string Id { get; set; }

        public BlobAccessPolicy AccessPolicy { get; set; }
    }
}
