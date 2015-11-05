using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using Basic.Azure.Storage.Communications.Utility;

namespace Basic.Azure.Storage.Communications.BlobService.ContainerOperations
{
    public class ListBlobsResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse, IReceiveDataWithResponse
    {
        public ListBlobsResponse()
        {
            BlobList = new ReadOnlyCollection<ListBlobsItem>(new List<ListBlobsItem>());
        }

        public virtual DateTime Date { get; protected set; }

        public virtual DateTime LastModified { get; protected set; }

        public virtual ReadOnlyCollection<ListBlobsItem> BlobList { get; protected set; }

        public virtual string Prefix { get; protected set; }

        public virtual string Marker { get; protected set; }

        public virtual int MaxResults { get; protected set; }

        public virtual string Delimiter { get; protected set; }

        public virtual string NextMarker { get; protected set; }

        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            //TODO: determine what we want to do about potential missing headers and date parsing errors

            Date = Parsers.ParseDateHeader(response.Headers[ProtocolConstants.Headers.OperationDate]);
            LastModified = Parsers.ParseDateHeader(response.Headers[ProtocolConstants.Headers.LastModified]);
        }

        public async Task ParseResponseBodyAsync(System.IO.Stream responseStream)
        {
            using (var sr = new StreamReader(responseStream))
            {
                var content = await sr.ReadToEndAsync();
                if (content.Length > 0)
                {
                    var xDoc = XDocument.Parse(content);

                    var blobs = xDoc.Root.Elements().FirstOrDefault(e => e.Name.LocalName.Equals("Blobs"));
                    if (blobs != null)
                    {
                        var blobCollection = blobs.Elements().Where(b => b.Name.LocalName.Equals("Blob"));
                        BlobList = new ReadOnlyCollection<ListBlobsItem>(ParseBlobs(blobCollection));
                    }

                    foreach (var field in xDoc.Root.Elements())
                    { 
                        switch(field.Name.LocalName)
                        {
                            case "Prefix":
                                Prefix = field.Value;
                                break;
                            case "Marker":
                                Marker = field.Value;
                                break;
                            case "MaxResults":
                                MaxResults = int.Parse(field.Value);
                                break;
                            case "Delimiter":
                                Delimiter = field.Value;
                                break;
                            case "NextMarker":
                                NextMarker = field.Value;
                                break;
                        }
                    }
                }
            }
        }

        private List<ListBlobsItem> ParseBlobs(IEnumerable<XElement> blobCollection)
        {
            var blobs = new List<ListBlobsItem>();
            foreach (var blob in blobCollection)
            {
                var blobItem = new ListBlobsItem();
                foreach (var field in blob.Elements())
                {
                    switch (field.Name.LocalName)
                    { 
                        case "Name":
                            blobItem.Name = field.Value;
                            break;
                        case "Snapshot":
                            blobItem.Snapshot = DateTime.Parse(field.Value, null, System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal);
                            break;
                        case "Properties":
                            blobItem.Properties = ParseBlobProperties(field);
                            break;
                        case "Metadata":
                            blobItem.Metadata = ParseBlobMetadata(field);
                            break;

                    }
                }
                blobs.Add(blobItem);
            }
            return blobs;
        }

        private static ListBlobsItemProperties ParseBlobProperties(XElement propertiesElement)
        {
            var properties = new ListBlobsItemProperties();
            foreach (var field in propertiesElement.Elements())
            {
                switch (field.Name.LocalName)
                {
                    case "Last-Modified":
                        properties.LastModified = DateTime.Parse(field.Value, null, System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal);
                        break;
                    case "Etag":
                        properties.ETag = field.Value;
                        break;
                    case "Content-Length":
                        properties.ContentLength = long.Parse(field.Value);
                        break;
                    case "Content-Type":
                        properties.ContentType = field.Value;
                        break;
                    case "Content-Encoding":
                        properties.ContentEncoding = field.Value;
                        break;
                    case "Content-Language":
                        properties.ContentLanguage = field.Value;
                        break;
                    case "Content-MD5":
                        properties.ContentMD5 = field.Value;
                        break;
                    case "Cache-Control":
                        properties.CacheControl = field.Value;
                        break;
                    case "x-ms-blob-sequence-number":
                        properties.BlobSequenceNumber = field.Value;
                        break;
                    case "BlobType":
                        if (field.Value.Equals("BlockBlob", StringComparison.InvariantCultureIgnoreCase))
                        {
                            properties.BlobType = Common.BlobType.Block;
                        }
                        else
                        {
                            properties.BlobType = Common.BlobType.Page;
                        }
                        break;
                    case "LeaseStatus":
                        if (field.Value.Equals("locked"))
                        {
                            properties.LeaseStatus = LeaseStatus.Locked;
                        }
                        else
                        {
                            properties.LeaseStatus = LeaseStatus.Unlocked;
                        }
                        break;
                    case "LeaseState":
                        switch (field.Value)
                        {
                            case "available":
                                properties.LeaseState = LeaseState.Available;
                                break;
                            case "leased":
                                properties.LeaseState = LeaseState.Leased;
                                break;
                            case "expired":
                                properties.LeaseState = LeaseState.Expired;
                                break;
                            case "breaking":
                                properties.LeaseState = LeaseState.Breaking;
                                break;
                            case "broken":
                                properties.LeaseState = LeaseState.Broken;
                                break;
                            default:
                                throw new AzureResponseParseException(field.Name.LocalName, field.Value);
                        }
                        break;
                    case "LeaseDuration":
                        switch (field.Value)
                        {
                            case "infinite":
                                properties.LeaseDuration = LeaseDuration.Infinite;
                                break;
                            case "fixed":
                                properties.LeaseDuration = LeaseDuration.Fixed;
                                break;
                            default:
                                throw new AzureResponseParseException(field.Name.LocalName, field.Value);
                        }
                        break;
                    case "CopyId":
                        properties.CopyId = field.Value;
                        break;
                    case "CopyStatus":
                        switch (field.Value)
                        {
                            case "pending":
                                properties.CopyStatus = CopyStatus.Pending;
                                break;
                            case "success":
                                properties.CopyStatus = CopyStatus.Success;
                                break;
                            case "aborted":
                                properties.CopyStatus = CopyStatus.Aborted;
                                break;
                            case "failed":
                                properties.CopyStatus = CopyStatus.Failed;
                                break;
                            default:
                                throw new AzureResponseParseException(field.Name.LocalName, field.Value);
                        }
                        break;
                    case "CopySource":
                        properties.CopySource = new Uri(field.Value);
                        break;
                    case "CopyProgress":
                        properties.CopyProgress = field.Value;
                        break;
                    case "CopyCompletionTime":
                        properties.CopyCompletionTime = DateTime.Parse(field.Value, null, System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal);
                        break;
                    case "CopyStatusDescription":
                        properties.CopyStatusDescription = field.Value;
                        break;

                }
            }
            return properties;
        }

        private static Dictionary<string, string> ParseBlobMetadata(XElement metadata)
        {
            return metadata
                .Elements()
                .ToDictionary(field => field.Name.LocalName, field => field.Value);
        }
    }
}
