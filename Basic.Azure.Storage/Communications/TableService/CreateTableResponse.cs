using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Basic.Azure.Storage.Communications.TableService
{
    public class CreateTableResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse, IReceiveDataWithResponse
    {
        public string Link { get; private set; }
        public MetadataPreference? MetadataPreferenceApplied { get; private set; }

        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
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

        public async Task ParseResponseBodyAsync(System.IO.Stream responseStream)
        {
            using (StreamReader sr = new StreamReader(responseStream))
            {
                var content = await sr.ReadToEndAsync();

                //TODO: add test with non-XML response so we can wrap that in a readable error
                var doc = XDocument.Parse(content);

                try
                {
                    Link = doc.Root.Elements()
                              .Where(e => e.Name.LocalName.Equals("id"))
                              .Single()
                              .Value;
                }
                catch
                { 
                }
            }
        }

    }
}
