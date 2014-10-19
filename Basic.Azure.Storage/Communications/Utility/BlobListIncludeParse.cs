using Basic.Azure.Storage.Communications.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Utility
{
    public static class BlobListIncludeParse
    {

        public static string ConvertToString(ListBlobsInclude include)
        {
            var options = new List<string>();

            if (include.HasFlag(ListBlobsInclude.Snapshots))
                options.Add("snapshots");

            if (include.HasFlag(ListBlobsInclude.Metadata))
                options.Add("metadata");

            if (include.HasFlag(ListBlobsInclude.UncomittedBlobs))
                options.Add("uncommittedblobs");

            if (include.HasFlag(ListBlobsInclude.Copy))
                options.Add("copy");

            return String.Join(",", options);
        }
    }
}
