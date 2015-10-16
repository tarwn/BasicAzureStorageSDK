using System;
using System.Collections.Generic;

namespace Basic.Azure.Storage.Communications.Core
{
    public class RetriedException : Exception
    {
        public int Count { get; set; }
        public Stack<Exception> ExceptionRetryStack { get; private set; }

        public RetriedException(Exception finalException, int tryCount, Stack<Exception> exceptionRetryStack = null)
            : base(String.Format("Exception after {0} attempts ({1} retries)", tryCount, tryCount - 1), finalException)
        {
            Count = tryCount;
            ExceptionRetryStack = exceptionRetryStack ?? new Stack<Exception>();
        }
    }
}
