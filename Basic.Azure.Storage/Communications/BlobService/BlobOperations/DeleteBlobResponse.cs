using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    public class DeleteBlobResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse
    {
        public DateTime Date { get; protected set; }

        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            //TODO: determine what we want to do about potential missing headers and date parsing errors
            Date = ParseDate(response.Headers[ProtocolConstants.Headers.OperationDate]);
        }

        private static DateTime ParseDate(string headerValue)
        {
            DateTime dateValue;
            DateTime.TryParse(headerValue, out dateValue);
            return dateValue;
        }
    }
}
