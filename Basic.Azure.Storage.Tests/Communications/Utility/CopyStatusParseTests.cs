using System;
using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.Utility;
using NUnit.Framework;

namespace Basic.Azure.Storage.Tests.Communications.Utility
{
    [TestFixture]
    public class CopyStatusParseTests
    {
        [Test]
        [TestCase("success", CopyStatus.Success)]
        [TestCase("pending", CopyStatus.Pending)]
        public void ParseCopyStatus_GivenCorrectStatus_ParsesCorrectly(string rawStatus, CopyStatus expectedParsedStatus)
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