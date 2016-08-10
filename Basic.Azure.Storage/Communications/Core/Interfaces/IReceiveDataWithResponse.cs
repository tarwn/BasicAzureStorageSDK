using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Core.Interfaces
{
    public interface IReceiveDataWithResponse
    {
        Task ParseResponseBodyAsync(Stream responseStream, string contentType);
    }
}
