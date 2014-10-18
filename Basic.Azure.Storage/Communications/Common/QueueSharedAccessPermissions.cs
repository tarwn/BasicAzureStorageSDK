using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Common
{
    [Flags]
    public enum QueueSharedAccessPermissions
    {
        None = 0,
        Add = 1,
        Update = 2,
        Read = 4,
        Write = 8
    }
}
