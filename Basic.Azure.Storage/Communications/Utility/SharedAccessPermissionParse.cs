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
        public static QueueSharedAccessPermissions ParseQueue(string permissions)
        {
            QueueSharedAccessPermissions result = QueueSharedAccessPermissions.None;

            if (permissions.Contains('a'))
                result |= QueueSharedAccessPermissions.Add;

            if (permissions.Contains('u'))
                result |= QueueSharedAccessPermissions.Update;

            if (permissions.Contains('r'))
                result |= QueueSharedAccessPermissions.Read;

            if (permissions.Contains('w'))
                result |= QueueSharedAccessPermissions.Write;

            return result;
        }

        public static string ConvertToString(QueueSharedAccessPermissions sharedAccessPermissions)
        {
            var result = new StringBuilder();

            if (sharedAccessPermissions.HasFlag(QueueSharedAccessPermissions.Add))
                result.Append("a");

            if (sharedAccessPermissions.HasFlag(QueueSharedAccessPermissions.Update))
                result.Append("u");

            if (sharedAccessPermissions.HasFlag(QueueSharedAccessPermissions.Read))
                result.Append("r");

            if (sharedAccessPermissions.HasFlag(QueueSharedAccessPermissions.Write))
                result.Append("w");

            return result.ToString();
        }

        public static string ConvertToString(BlobSharedAccessPermissions sharedAccessPermissions)
        {
            var result = new StringBuilder();

            if (sharedAccessPermissions.HasFlag(BlobSharedAccessPermissions.Read))
                result.Append("r");

            if (sharedAccessPermissions.HasFlag(BlobSharedAccessPermissions.Write))
                result.Append("w");

            if (sharedAccessPermissions.HasFlag(BlobSharedAccessPermissions.Delete))
                result.Append("d");

            if (sharedAccessPermissions.HasFlag(BlobSharedAccessPermissions.List))
                result.Append("l");

            return result.ToString();
        }

        public static BlobSharedAccessPermissions ParseBlob(string permissions)
        {
            BlobSharedAccessPermissions result = BlobSharedAccessPermissions.None;

            if (permissions.Contains('r'))
                result |= BlobSharedAccessPermissions.Read;

            if (permissions.Contains('w'))
                result |= BlobSharedAccessPermissions.Write;

            if (permissions.Contains('d'))
                result |= BlobSharedAccessPermissions.Delete;

            if (permissions.Contains('l'))
                result |= BlobSharedAccessPermissions.List;

            return result;
        }
    }
}
