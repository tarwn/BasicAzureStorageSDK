using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Basic.Azure.Storage.Communications.Utility;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Basic.Azure.Storage.Communications.BlobService.ContainerOperations
{
    public class GetContainerACLResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse, IReceiveDataWithResponse
    {
        public GetContainerACLResponse()
        {
            SignedIdentifiers = new ReadOnlyCollection<BlobSignedIdentifier>(new List<BlobSignedIdentifier>());
        }

        public virtual ReadOnlyCollection<BlobSignedIdentifier> SignedIdentifiers { get; protected set; }

        public virtual ContainerAccessType PublicAccess { get; set; }


        public virtual DateTime Date { get; protected set; }

        public virtual string ETag { get; protected set; }

        public virtual DateTime LastModified { get; protected set; }

        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            //TODO: determine what we want to do about potential missing headers and date parsing errors

            ETag = response.Headers[ProtocolConstants.Headers.ETag].Trim('"');
            Date = DateParse.ParseHeader(response.Headers[ProtocolConstants.Headers.OperationDate]);
            LastModified = DateParse.ParseHeader(response.Headers[ProtocolConstants.Headers.LastModified]);

            if (response.Headers[ProtocolConstants.Headers.BlobPublicAccess] == null)
            {
                PublicAccess = ContainerAccessType.None;
            }
            else
            {
                switch (response.Headers[ProtocolConstants.Headers.BlobPublicAccess])
                {
                    case ProtocolConstants.HeaderValues.BlobPublicAccess.Container:
                        PublicAccess = ContainerAccessType.PublicContainer;
                        break;
                    case ProtocolConstants.HeaderValues.BlobPublicAccess.Blob:
                        PublicAccess = ContainerAccessType.PublicBlob;
                        break;
                    default:
                        throw new AzureResponseParseException(ProtocolConstants.Headers.BlobPublicAccess, response.Headers[ProtocolConstants.Headers.BlobPublicAccess]);
                }
            }
        }

        public async Task ParseResponseBodyAsync(System.IO.Stream responseStream)
        {
            using (StreamReader sr = new StreamReader(responseStream))
            {
                var content = await sr.ReadToEndAsync();
                if (content.Length > 0)
                {
                    var xDoc = XDocument.Parse(content);
                    var signedIdentifiers = new List<BlobSignedIdentifier>();

                    foreach (var identifierResponse in xDoc.Root.Elements().Where(e => e.Name.LocalName.Equals("SignedIdentifier")))
                    {
                        var identifier = new BlobSignedIdentifier();
                        identifier.AccessPolicy = new BlobAccessPolicy();

                        foreach (var element in identifierResponse.Elements())
                        {
                            switch (element.Name.LocalName)
                            {
                                case "Id":
                                    identifier.Id = element.Value;
                                    break;
                                case "AccessPolicy":
                                    foreach (var apElement in element.Elements())
                                    {
                                        switch (apElement.Name.LocalName)
                                        {
                                            case "Permission":
                                                identifier.AccessPolicy.Permission = SharedAccessPermissionParse.ParseBlob(apElement.Value);
                                                break;
                                            case "Start":
                                                identifier.AccessPolicy.StartTime = DateParse.ParseUTC(apElement.Value);
                                                break;
                                            case "Expiry":
                                                identifier.AccessPolicy.Expiry = DateParse.ParseUTC(apElement.Value);
                                                break;
                                        }
                                    }
                                    break;
                            }
                        }

                        signedIdentifiers.Add(identifier);
                    }

                    SignedIdentifiers = new ReadOnlyCollection<BlobSignedIdentifier>(signedIdentifiers);
                }
            }
        }
    }
}
