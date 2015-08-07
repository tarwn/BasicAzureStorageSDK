using Basic.Azure.Storage.Communications.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.BlobService
{
    public class BlobRange
    {
        private long _startByte;
        private long? _endByte;

        public BlobRange(long startByte)
        {
            Guard.ArgumentInRanges("startByte", startByte, new GuardRange<long>(0, long.MaxValue));

            _startByte = startByte;
        }

        public BlobRange(long startByte, long endByte)
        {
            Guard.ArgumentInRanges("startByte", startByte, new GuardRange<long>(0, long.MaxValue));
            Guard.ArgumentGreaterThan("endByte", endByte, "startByte", startByte);

            _startByte = startByte;
            _endByte = endByte;
        }

        public string GetStringValue()
        {
            if (_endByte.HasValue)
            { 
                return String.Format("bytes={0}-{1}", _startByte, _endByte);
            }
            else
            {
                return String.Format("bytes={0}-", _startByte);
            }
        }
    }
}
