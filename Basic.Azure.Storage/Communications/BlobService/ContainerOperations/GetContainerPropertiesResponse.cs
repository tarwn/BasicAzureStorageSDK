using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.BlobService.ContainerOperations
{
    public class GetContainerPropertiesResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse
    {
        public DateTime Date { get; protected set; }

        public string ETag { get; protected set; }

        public DateTime LastModified { get; protected set; }



        public ReadOnlyDictionary<string, string> Metadata { get; protected set; }
        public LeaseDuration LeaseDuration { get; protected set; }
        public LeaseStatus LeaseStatus { get; protected set; }
        public LeaseState LeaseState { get; protected set; }


        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            //TODO: determine what we want to do about potential missing headers and date parsing errors

            ETag = response.Headers[ProtocolConstants.Headers.ETag].Trim(new char[] { '"' });
            Date = ParseDate(response.Headers[ProtocolConstants.Headers.OperationDate]);
            LastModified = ParseDate(response.Headers[ProtocolConstants.Headers.LastModified]);

            switch (response.Headers[ProtocolConstants.Headers.LeaseStatus])
            {
                case ProtocolConstants.HeaderValues.LeaseStatus.Locked:
                    LeaseStatus = LeaseStatus.Locked;
                    break;
                case ProtocolConstants.HeaderValues.LeaseStatus.Unlocked:
                    LeaseStatus = LeaseStatus.Unlocked;
                    break;
                default:
                    LeaseStatus = LeaseStatus.Unknown;
                    break;
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
                    LeaseDuration = LeaseDuration.Unknown;
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

        private DateTime ParseDate(string headerValue)
        {
            DateTime dateValue;
            DateTime.TryParse(headerValue, out dateValue);
            return dateValue;
        }

    }
}
