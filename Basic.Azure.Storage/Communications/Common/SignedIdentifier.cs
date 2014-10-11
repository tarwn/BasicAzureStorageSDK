using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Common
{
    public class SignedIdentifier
    {
        public string Id { get; set; }

        public AccessPolicy AccessPolicy { get; set; }
    }
}
