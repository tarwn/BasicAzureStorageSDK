using Basic.Azure.Storage.Communications.BlobService.ContainerOperations;
using Basic.Azure.Storage.Tests.Fakes;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using TestableHttpWebResponse;

namespace Basic.Azure.Storage.Tests.Communications.BlobService.ContainerOperations
{
    [TestFixture]
    public class LeaseContainerAcquireTests
    {
        [TestCase(-2)]
        [TestCase(0)]
        [TestCase(14)]
        [TestCase(61)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void Ctor_InvalidLeaseDuration_ThrowsException(int leaseDuration)
        { 
            var request = new LeaseContainerAcquireRequest(new SettingsFake(), "container", leaseDuration);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void Ctor_InvalidProposedLease_ThrowsException()
        { 
            var request = new LeaseContainerAcquireRequest(new SettingsFake(), "container", -1, "abc-123");
        }


        [TestCase(null)]
        [TestCase("")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void Ctor_EmptyContainerName_ThrowsException(string containerName)
        {
            var request = new LeaseContainerAcquireRequest(new SettingsFake(), containerName, -1);
        }
    }
}
