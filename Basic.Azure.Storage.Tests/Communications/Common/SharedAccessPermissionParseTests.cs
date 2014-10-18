using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.Utility;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Tests.Communications.Common
{
    [TestFixture]
    public class SharedAccessPermissionParseTests
    {

        [TestCase("a", QueueSharedAccessPermissions.Add)]
        [TestCase("u", QueueSharedAccessPermissions.Update)]
        [TestCase("r", QueueSharedAccessPermissions.Read)]
        [TestCase("w", QueueSharedAccessPermissions.Write)]
        public void Parse_IndividualCode_ConvertsToMatchingEnum(string code, QueueSharedAccessPermissions expectedPermission)
        {
            var result = SharedAccessPermissionParse.ParseQueue(code);
            Assert.AreEqual(expectedPermission, result);
        }

        [Test]
        public void Parse_CombinedCode_ConvertsToMatchingFlagEnum()
        {
            var expectedPermission = QueueSharedAccessPermissions.Add | QueueSharedAccessPermissions.Read | QueueSharedAccessPermissions.Update | QueueSharedAccessPermissions.Write;

            var result = SharedAccessPermissionParse.ParseQueue("rwau");

            Assert.AreEqual(expectedPermission, result);
        }

        [TestCase(QueueSharedAccessPermissions.Add, "a")]
        [TestCase(QueueSharedAccessPermissions.Update, "u")]
        [TestCase(QueueSharedAccessPermissions.Read, "r")]
        [TestCase(QueueSharedAccessPermissions.Write, "w")]
        public void ConvertToString_IndividualEnum_ConvertsToMatchingLetter(QueueSharedAccessPermissions permission, string expectedCode)
        {
            var result = SharedAccessPermissionParse.ConvertToString(permission);
            Assert.AreEqual(expectedCode, result);
        }

        [TestCase(QueueSharedAccessPermissions.Add | QueueSharedAccessPermissions.Read, "ar")]
        [TestCase(QueueSharedAccessPermissions.Update | QueueSharedAccessPermissions.Write, "uw")]
        [TestCase(QueueSharedAccessPermissions.Add | QueueSharedAccessPermissions.Read | QueueSharedAccessPermissions.Update | QueueSharedAccessPermissions.Write, "aurw")]
        public void ConvertToString_FlagEnum_ConvertsToStringWithMatchingCodes(QueueSharedAccessPermissions permission, string expectedCode)
        {
            var result = SharedAccessPermissionParse.ConvertToString(permission);
            Assert.AreEqual(expectedCode, result);
        }

    }
}
