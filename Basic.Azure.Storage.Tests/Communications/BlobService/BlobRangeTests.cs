using Basic.Azure.Storage.Communications.BlobService;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Tests.Communications.BlobService
{
    [TestFixture]
    public class BlobRangeTests
    {

        [Test]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void Ctor_InvalidRange_ThrowsArgumentException()
        {
            long start = 5, end = 3;

            var range = new BlobRange(start, end);

            //expected exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void Ctor_NegativeStart_ThrowsArgumentException()
        {
            long start = -1, end = 3;

            var range = new BlobRange(start, end);

            //expected exception
        }

        [Test]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void Ctor_NegativeStartWithoutEnd_ThrowsArgumentException()
        {
            long start = -1;

            var range = new BlobRange(start);

            //expected exception
        }

        [Test]
        public void GetStringValue_ValidStartByte_FormatProperlyForAzureRangeArgument()
        {
            long start = 5;
            var range = new BlobRange(start);

            var rangeValue = range.GetStringValue();

            Assert.AreEqual("bytes=5-", rangeValue);
        }

        [Test]
        public void GetStringValue_ValidStartByteAndEndByte_FormatProperlyForAzureRangeArgument()
        {
            long start = 5, end = 10;
            var range = new BlobRange(start, end);

            var rangeValue = range.GetStringValue();

            Assert.AreEqual("bytes=5-10", rangeValue);
        }
    }
}
