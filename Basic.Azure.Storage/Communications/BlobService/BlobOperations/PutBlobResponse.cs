using Basic.Azure.Storage.Communications.Core;
using System;
using Basic.Azure.Storage.Communications.Core.Interfaces;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    public class PutBlobResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse, IBlobPropertiesResponse
    {
        public virtual string ETag { get; protected set; }

        public virtual DateTime LastModified { get; protected set; }

        public virtual DateTime Date { get; protected set; }

        public virtual string ContentMD5 { get; protected set; }


        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            //TODO: determine what we want to do about potential missing headers and date parsing errors

            ETag = response.Headers[ProtocolConstants.Headers.ETag].Trim(new char[] { '"' });
            Date = ParseDate(response.Headers[ProtocolConstants.Headers.OperationDate]);
            LastModified = ParseDate(response.Headers[ProtocolConstants.Headers.LastModified]);
            ContentMD5 = response.Headers[ProtocolConstants.Headers.ContentMD5];
        }

        private DateTime ParseDate(string headerValue)
        {
            DateTime dateValue;
            DateTime.TryParse(headerValue, out dateValue);
            return dateValue;
        }

    }
}
