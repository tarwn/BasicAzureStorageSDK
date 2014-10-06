using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.QueueService.QueueOperations
{
    public class GetQueueMetadataResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse
    {
        public int ApproximateMessageCount { get; protected set; }

        public ReadOnlyDictionary<string, string> Metadata { get; protected set; }

        public string RequestId { get; protected set; }

        public string Version { get; protected set; }

        public DateTime Date { get; protected set; }

        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            //TODO: determine what we want to do about potential missing headers and date parsing errors

            RequestId = response.Headers[ProtocolConstants.Headers.RequestId];
            Version = response.Headers[ProtocolConstants.Headers.Version];
            Date = ParseDate(response.Headers[ProtocolConstants.Headers.OperationDate]);

            ApproximateMessageCount = int.Parse(response.Headers[ProtocolConstants.Headers.ApproximateMessagesCount]);

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
