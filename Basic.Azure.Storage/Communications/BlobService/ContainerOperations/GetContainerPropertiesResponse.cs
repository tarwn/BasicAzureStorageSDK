using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Basic.Azure.Storage.Communications.Utility;

namespace Basic.Azure.Storage.Communications.BlobService.ContainerOperations
{
    public class GetContainerPropertiesResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse
    {
        public virtual DateTime Date { get; protected set; }

        public virtual string ETag { get; protected set; }

        public virtual DateTime LastModified { get; protected set; }

        public ReadOnlyDictionary<string, string> Metadata { get; protected set; }
        public LeaseDuration LeaseDuration { get; protected set; }
        public LeaseStatus LeaseStatus { get; protected set; }
        public LeaseState LeaseState { get; protected set; }

        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            //TODO: determine what we want to do about potential missing headers and date parsing errors

            ETag = response.Headers[ProtocolConstants.Headers.ETag].Trim('"');
            Date = DateParse.ParseHeader(response.Headers[ProtocolConstants.Headers.OperationDate]);
            LastModified = DateParse.ParseHeader(response.Headers[ProtocolConstants.Headers.LastModified]);

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

            var metadata = new Dictionary<string, string>();
            foreach (var headerKey in response.Headers.AllKeys)
            {
                if (headerKey.StartsWith(ProtocolConstants.Headers.MetaDataPrefix, StringComparison.InvariantCultureIgnoreCase))
                {
                    metadata[headerKey.Substring(ProtocolConstants.Headers.MetaDataPrefix.Length)] = response.Headers[headerKey];
                }
            }
            Metadata = new ReadOnlyDictionary<string, string>(metadata);
        }

    }
}
