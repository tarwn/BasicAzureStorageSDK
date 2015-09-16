using System;
using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    public class LeaseBlobRenewResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse
    {
        public DateTime Date { get; protected set; }

        // 2013-0815 forward - public string ETag { get; protected set; }

        public DateTime LastModified { get; protected set; }

        public string LeaseId { get; protected set; }

        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            //TODO: determine what we want to do about potential missing headers and date parsing errors

            // 2013-0815 forward - ETag = response.Headers[ProtocolConstants.Headers.ETag].Trim(new char[] { '"' });
            Date = ParseDate(response.Headers[ProtocolConstants.Headers.OperationDate]);
            LastModified = ParseDate(response.Headers[ProtocolConstants.Headers.LastModified]);

            LeaseId = response.Headers[ProtocolConstants.Headers.LeaseId];
        }

        private DateTime ParseDate(string headerValue)
        {
            DateTime dateValue;
            DateTime.TryParse(headerValue, out dateValue);
            return dateValue;
        }
    }
}
