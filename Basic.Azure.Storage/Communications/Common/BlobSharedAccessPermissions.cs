using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Common
{
    [Flags]
    public enum BlobSharedAccessPermissions
    {
        None = 0,
        Read = 1,
        Write = 2,
        Delete = 4,
        List = 8
    }
}
