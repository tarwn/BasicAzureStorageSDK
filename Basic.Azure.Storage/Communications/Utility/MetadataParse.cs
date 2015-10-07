using System;
using System.Collections.ObjectModel;
using System.Linq;
using System.Net;
using Basic.Azure.Storage.Communications.Core;

namespace Basic.Azure.Storage.Communications.Utility
{
    public static class MetadataParse
    {

        public static ReadOnlyDictionary<string, string> ParseMetadata(WebResponse response)
        {
            var parsedMetadata = response.Headers.AllKeys
               .Where(key => key.StartsWith(ProtocolConstants.Headers.MetaDataPrefix, StringComparison.InvariantCultureIgnoreCase))
               .ToDictionary(key => key.Substring(ProtocolConstants.Headers.MetaDataPrefix.Length), key => response.Headers[key]);

            return new ReadOnlyDictionary<string, string>(parsedMetadata);
        }

    }
}