using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Basic.Azure.Storage.Communications.QueueService.QueueOperations
{
    public class GetQueueACLResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse, IReceiveDataWithResponse
    {
        public GetQueueACLResponse()
        {
            SignedIdentifiers = new ReadOnlyCollection<QueueSignedIdentifier>(new List<QueueSignedIdentifier>());
        }

        public virtual ReadOnlyCollection<QueueSignedIdentifier> SignedIdentifiers { get; protected set; }

        public virtual string RequestId { get; protected set; }

        public virtual string Version { get; protected set; }

        public virtual DateTime Date { get; protected set; }

        private DateTime ParseDate(string headerValue)
        {
            DateTime dateValue;
            DateTime.TryParse(headerValue, out dateValue);
            return dateValue;
        }

        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            //TODO: determine what we want to do about potential missing headers and date parsing errors

            RequestId = response.Headers[ProtocolConstants.Headers.RequestId];
            Version = response.Headers[ProtocolConstants.Headers.Version];
            Date = ParseDate(response.Headers[ProtocolConstants.Headers.OperationDate]);
        }

        public async Task ParseResponseBodyAsync(System.IO.Stream responseStream)
        {
            using (StreamReader sr = new StreamReader(responseStream))
            {
                var content = await sr.ReadToEndAsync();
                if (content.Length > 0)
                {
                    var xDoc = XDocument.Parse(content);
                    var signedIdentifiers = new List<QueueSignedIdentifier>();

                    foreach (var identifierResponse in xDoc.Root.Elements().Where(e => e.Name.LocalName.Equals("SignedIdentifier")))
                    {
                        var identifier = new QueueSignedIdentifier();
                        identifier.AccessPolicy = new QueueAccessPolicy();

                        foreach (var element in identifierResponse.Elements())
                        {
                            switch (element.Name.LocalName) { 
                                case "Id":
                                    identifier.Id = element.Value;
                                    break;
                                case "AccessPolicy":
                                    foreach (var apElement in element.Elements())
                                    {
                                        switch (apElement.Name.LocalName) { 
                                            case "Permission":
                                                identifier.AccessPolicy.Permission = SharedAccessPermissionParse.ParseQueue(apElement.Value);
                                                break;
                                            case "Start":
                                                identifier.AccessPolicy.StartTime = Parsers.ParseUTCDate(apElement.Value);
                                                break;
                                            case "Expiry":
                                                identifier.AccessPolicy.Expiry = Parsers.ParseUTCDate(apElement.Value);
                                                break;
                                        }
                                    }
                                    break;
                            }
                        }

                        signedIdentifiers.Add(identifier);
                    }

                    SignedIdentifiers = new ReadOnlyCollection<QueueSignedIdentifier>(signedIdentifiers);
                }
            }
        }
    }
}
