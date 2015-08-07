using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Tests.Communications.Utility
{
    [TestFixture]
    public class GuardTests
    {

        [Test]
        public void ArgumentGreaterThan_FirstLargerThanSecond_DoesNotThrowException()
        {
            int min = 1, max = 2;

            Basic.Azure.Storage.Communications.Utility.Guard.ArgumentGreaterThan("max", max, "min", min);

            // no exception expected
        }

        [Test]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void ArgumentGreaterThan_FirstSmallerThanSecond_ThrowsException()
        {
            int min = 5, max = 2;

            Basic.Azure.Storage.Communications.Utility.Guard.ArgumentGreaterThan("max", max, "min", min);

            // no exception expected
        }

        [Test]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void ArgumentGreaterThan_EqualArgs_ThrowsException()
        {
            int min = 1, max = 1;

            Basic.Azure.Storage.Communications.Utility.Guard.ArgumentGreaterThan("max", max, "min", min);

            // no exception expected
        }

    }
}
