using Basic.Azure.Storage.Communications.Core;
using System;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;
using Basic.Azure.Storage.Extensions.Contracts;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    public class PutBlobResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse, IBlobOrBlockListResponseWrapper
    {
        public virtual string ETag { get; protected set; }

        public virtual DateTime LastModified { get; protected set; }

        public virtual DateTime Date { get; protected set; }

        public virtual string ContentMD5 { get; protected set; }


        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            //TODO: determine what we want to do about potential missing headers and date parsing errors

            ETag = response.Headers[ProtocolConstants.Headers.ETag].Trim(new char[] { '"' });
            Date = Parsers.ParseDateHeader(response.Headers[ProtocolConstants.Headers.OperationDate]);
            LastModified = Parsers.ParseDateHeader(response.Headers[ProtocolConstants.Headers.LastModified]);
            ContentMD5 = response.Headers[ProtocolConstants.Headers.ContentMD5];
        }

    }
}
