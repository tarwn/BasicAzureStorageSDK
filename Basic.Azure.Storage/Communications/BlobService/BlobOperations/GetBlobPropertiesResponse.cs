using System;
using System.Collections.ObjectModel;
using System.Linq;
using System.Net;
using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Basic.Azure.Storage.Communications.Utility;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    public class GetBlobPropertiesResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse
    {
        public virtual DateTime LastModified { get; protected set; }

        public virtual ReadOnlyDictionary<string, string> Metadata { get; protected set; }

        public virtual BlobType BlobType { get; protected set; }

        public virtual LeaseDuration LeaseDuration { get; protected set; }

        public virtual LeaseState LeaseState { get; protected set; }

        public virtual LeaseStatus LeaseStatus { get; protected set; }

        public virtual int ContentLength { get; protected set; }

        public virtual string ContentType { get; protected set; }

        public virtual string ETag { get; protected set; }

        public virtual string ContentMD5 { get; protected set; }

        public virtual string ContentEncoding { get; protected set; }

        public virtual string ContentLanguage { get; protected set; }

        public virtual DateTime Date { get; protected set; }

        public void ParseHeaders(HttpWebResponse response)
        {
            //TODO: determine what we want to do about potential missing headers and date parsing errors

            LastModified = DateParse.ParseHeader(response.Headers[ProtocolConstants.Headers.LastModified]);
            ContentLength = int.Parse(response.Headers[ProtocolConstants.Headers.ContentLength]);
            ContentType = response.Headers[ProtocolConstants.Headers.ContentType];
            ContentMD5 = response.Headers[ProtocolConstants.Headers.ContentMD5];
            ContentEncoding = response.Headers[ProtocolConstants.Headers.ContentEncoding];
            ContentLanguage = response.Headers[ProtocolConstants.Headers.ContentLanguage];
            ETag = response.Headers[ProtocolConstants.Headers.ETag].Trim('"');
            Date = DateParse.ParseHeader(response.Headers[ProtocolConstants.Headers.OperationDate]);

            ParseBlobType(response);

            ParseLeaseStatus(response);

            ParseLeaseState(response);

            ParseLeaseDuration(response);

            Metadata = MetadataParse.ParseMetadata(response);
        }

        private void ParseBlobType(WebResponse response)
        {
            switch (response.Headers[ProtocolConstants.Headers.BlobType])
            {
                case ProtocolConstants.HeaderValues.BlobType.Block:
                    BlobType = BlobType.Block;
                    break;
                case ProtocolConstants.HeaderValues.BlobType.Page:
                    BlobType = BlobType.Page;
                    break;
                default:
                    throw new ArgumentException("Reponse signifies unsupported blob type", "response");
            }
        }

        private void ParseLeaseDuration(WebResponse response)
        {
            switch (response.Headers[ProtocolConstants.Headers.LeaseDuration])
            {
                case ProtocolConstants.HeaderValues.LeaseDuration.Fixed:
                    LeaseDuration = LeaseDuration.Fixed;
                    break;
                case ProtocolConstants.HeaderValues.LeaseDuration.Infinite:
                    LeaseDuration = LeaseDuration.Infinite;
                    break;
                default:
                    LeaseDuration = LeaseDuration.NotSpecified;
                    break;
            }
        }

        private void ParseLeaseState(WebResponse response)
        {
            switch (response.Headers[ProtocolConstants.Headers.LeaseState])
            {
                case ProtocolConstants.HeaderValues.LeaseState.Available:
                    LeaseState = Common.LeaseState.Available;
                    break;
                case ProtocolConstants.HeaderValues.LeaseState.Breaking:
                    LeaseState = Common.LeaseState.Breaking;
                    break;
                case ProtocolConstants.HeaderValues.LeaseState.Broken:
                    LeaseState = Common.LeaseState.Broken;
                    break;
                case ProtocolConstants.HeaderValues.LeaseState.Expired:
                    LeaseState = Common.LeaseState.Expired;
                    break;
                case ProtocolConstants.HeaderValues.LeaseState.Leased:
                    LeaseState = Common.LeaseState.Leased;
                    break;
                default:
                    throw new AzureResponseParseException(ProtocolConstants.Headers.LeaseState, response.Headers[ProtocolConstants.Headers.LeaseState]);
            }
        }

        private void ParseLeaseStatus(WebResponse response)
        {
            switch (response.Headers[ProtocolConstants.Headers.LeaseStatus])
            {
                case ProtocolConstants.HeaderValues.LeaseStatus.Locked:
                    LeaseStatus = LeaseStatus.Locked;
                    break;
                case ProtocolConstants.HeaderValues.LeaseStatus.Unlocked:
                    LeaseStatus = LeaseStatus.Unlocked;
                    break;
                default:
                    throw new AzureResponseParseException(ProtocolConstants.Headers.LeaseStatus, response.Headers[ProtocolConstants.Headers.LeaseStatus]);
            }
        }
    }
}
