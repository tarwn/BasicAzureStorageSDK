using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Common
{
    public enum LeaseState
    {
        Available,
        Leased,
        Expired,
        Broken,
        Breaking
    }
}
