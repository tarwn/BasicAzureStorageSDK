using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Core.Interfaces
{
    public interface ISendAdditionalRequiredHeaders
    {
        void ApplyAdditionalRequiredHeaders(System.Net.WebRequest request);
    }
}
