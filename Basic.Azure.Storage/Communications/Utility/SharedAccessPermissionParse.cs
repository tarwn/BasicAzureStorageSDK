using Basic.Azure.Storage.Communications.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Utility
{
    public static class SharedAccessPermissionParse
    {
        public static SharedAccessPermissions Parse(string permissions)
        {
            SharedAccessPermissions result = SharedAccessPermissions.None;

            if (permissions.Contains('a'))
                result |= SharedAccessPermissions.Add;

            if (permissions.Contains('u'))
                result |= SharedAccessPermissions.Update;

            if (permissions.Contains('r'))
                result |= SharedAccessPermissions.Read;

            if (permissions.Contains('w'))
                result |= SharedAccessPermissions.Write;

            return result;
        }

        public static string ConvertToString(SharedAccessPermissions sharedAccessPermissions)
        {
            var result = new StringBuilder();

            if (sharedAccessPermissions.HasFlag(SharedAccessPermissions.Add))
                result.Append("a");

            if (sharedAccessPermissions.HasFlag(SharedAccessPermissions.Update))
                result.Append("u");

            if (sharedAccessPermissions.HasFlag(SharedAccessPermissions.Read))
                result.Append("r");

            if (sharedAccessPermissions.HasFlag(SharedAccessPermissions.Write))
                result.Append("w");

            return result.ToString();
        }
    }
}
