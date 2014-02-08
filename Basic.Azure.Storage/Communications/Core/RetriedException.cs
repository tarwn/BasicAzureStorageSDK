using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Basic.Azure.Storage.Communications.Core
{
    public class RetriedException : Exception
    {
        public int Count { get; set; }

        public RetriedException(Exception finalException, int tryCount)
            : base(String.Format("Exception after {0} attempts ({1} retries)", tryCount, tryCount - 1), finalException)
        {
            Count = tryCount;
        }
    }
}
