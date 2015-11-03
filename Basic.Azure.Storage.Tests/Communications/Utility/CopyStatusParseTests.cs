using System;
using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.Utility;
using NUnit.Framework;

namespace Basic.Azure.Storage.Tests.Communications.Utility
{
    [TestFixture]
    public class CopyStatusParseTests
    {
        [Test]
        [TestCase("success", BlobCopyStatus.Success)]
        [TestCase("pending", BlobCopyStatus.Pending)]
        public void ParseCopyStatus_GivenCorrectStatus_ParsesCorrectly(string rawStatus, BlobCopyStatus expectedParsedStatus)
        {
            var parsed = CopyStatusParse.ParseCopyStatus(rawStatus);

            Assert.AreEqual(parsed, expectedParsedStatus);
        }

        [Test]
        public void ParseCopyStatus_GivenInvalidStatus_ThrowsArgumentException()
        {
            const string invalidRawStatus = "impendingDoom";

            Assert.Throws<ArgumentException>(() =>
            {
                CopyStatusParse.ParseCopyStatus(invalidRawStatus);
            });
        }
    }
}