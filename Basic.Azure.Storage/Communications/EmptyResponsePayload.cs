using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications
{
    public class EmptyResponsePayload : IResponsePayload
    {
        public bool ExpectsResponseBody { get { return false; } }

        public void ParseResponseBody(System.IO.Stream responseStream)
        {
            throw new NotImplementedException("This payload does not expect a response payload.");
        }
    }
}
