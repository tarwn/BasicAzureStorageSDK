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

        [TestCase("a", SharedAccessPermissions.Add)]
        [TestCase("u", SharedAccessPermissions.Update)]
        [TestCase("r", SharedAccessPermissions.Read)]
        [TestCase("w", SharedAccessPermissions.Write)]
        public void Parse_IndividualCode_ConvertsToMatchingEnum(string code, SharedAccessPermissions expectedPermission)
        {
            var result = SharedAccessPermissionParse.Parse(code);
            Assert.AreEqual(expectedPermission, result);
        }

        [Test]
        public void Parse_CombinedCode_ConvertsToMatchingFlagEnum()
        {
            var expectedPermission = SharedAccessPermissions.Add | SharedAccessPermissions.Read | SharedAccessPermissions.Update | SharedAccessPermissions.Write;

            var result = SharedAccessPermissionParse.Parse("rwau");

            Assert.AreEqual(expectedPermission, result);
        }

        [TestCase(SharedAccessPermissions.Add, "a")]
        [TestCase(SharedAccessPermissions.Update, "u")]
        [TestCase(SharedAccessPermissions.Read, "r")]
        [TestCase(SharedAccessPermissions.Write, "w")]
        public void ConvertToString_IndividualEnum_ConvertsToMatchingLetter(SharedAccessPermissions permission, string expectedCode)
        {
            var result = SharedAccessPermissionParse.ConvertToString(permission);
            Assert.AreEqual(expectedCode, result);
        }

        [TestCase(SharedAccessPermissions.Add | SharedAccessPermissions.Read, "ar")]
        [TestCase(SharedAccessPermissions.Update | SharedAccessPermissions.Write, "uw")]
        [TestCase(SharedAccessPermissions.Add | SharedAccessPermissions.Read | SharedAccessPermissions.Update | SharedAccessPermissions.Write, "aurw")]
        public void ConvertToString_FlagEnum_ConvertsToStringWithMatchingCodes(SharedAccessPermissions permission, string expectedCode)
        {
            var result = SharedAccessPermissionParse.ConvertToString(permission);
            Assert.AreEqual(expectedCode, result);
        }

    }
}
