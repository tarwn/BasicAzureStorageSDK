using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Common
{
    public enum ListBlobsInclude
    {
        Snapshots = 1,
        Metadata = 2,
        UncomittedBlobs = 4,
        Copy = 8
    }
}
