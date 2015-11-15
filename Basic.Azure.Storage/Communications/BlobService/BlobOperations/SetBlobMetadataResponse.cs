using System;
using System.Net;
using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    public class SetBlobMetadataResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse
    {
        public virtual DateTime LastModified { get; protected set; }

        public virtual string ETag { get; protected set; }

        public virtual DateTime Date { get; protected set; }

        public void ParseHeaders(HttpWebResponse response)
        {
            //TODO: determine what we want to do about potential missing headers and date parsing errors

            LastModified = Parsers.ParseDateHeader(response.Headers[ProtocolConstants.Headers.LastModified]);
            ETag = response.Headers[ProtocolConstants.Headers.ETag].Trim('"');
            Date = Parsers.ParseDateHeader(response.Headers[ProtocolConstants.Headers.OperationDate]);
        }
    }
}
