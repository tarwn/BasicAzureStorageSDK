using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Basic.Azure.Storage.Communications.TableService.EntityOperations
{
    public class MergeEntityResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse
    {

        public string ETag { get; private set; }
        
        public MetadataPreference? MetadataPreferenceApplied { get; private set; }

        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            ETag = response.Headers[ProtocolConstants.Headers.ETag];

            if (response.Headers[ProtocolConstants.Headers.PreferenceApplied] == null)
            {
                MetadataPreferenceApplied = null;
            }
            else if (response.Headers[ProtocolConstants.Headers.PreferenceApplied].Equals(ProtocolConstants.HeaderValues.TableMetadataPreference.ReturnContent, StringComparison.InvariantCultureIgnoreCase))
            {
                MetadataPreferenceApplied = MetadataPreference.ReturnContent;
            }
            else
            {
                MetadataPreferenceApplied = MetadataPreference.ReturnNoContent;
            }

        }

    }
}
