using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;
using Basic.Azure.Storage.Communications.Utility;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    public class PutBlockResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse
    {
        public virtual DateTime Date { get; protected set; }

        public virtual string ContentMD5 { get; protected set; }

        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            //TODO: determine what we want to do about potential missing headers and date parsing errors

            Date = Parsers.ParseDateHeader(response.Headers[ProtocolConstants.Headers.OperationDate]);
            ContentMD5 = response.Headers[ProtocolConstants.Headers.ContentMD5];
        }

    }
}
