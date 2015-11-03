﻿using Basic.Azure.Storage.Communications.Core;
using System;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    public class CopyBlobResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse
    {
        public virtual string ETag { get; protected set; }

        public virtual DateTime LastModified { get; protected set; }

        public virtual DateTime Date { get; protected set; }

        public virtual string CopyId { get; protected set; }

        public virtual BlobCopyStatus CopyStatus { get; protected set; }

        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            //TODO: determine what we want to do about potential missing headers and date parsing errors

            ETag = response.Headers[ProtocolConstants.Headers.ETag].Trim('"');
            Date = DateParse.ParseHeader(response.Headers[ProtocolConstants.Headers.OperationDate]);
            LastModified = DateParse.ParseHeader(response.Headers[ProtocolConstants.Headers.LastModified]);
            CopyId = response.Headers[ProtocolConstants.Headers.CopyId];
            CopyStatus = CopyStatusParse.ParseCopyStatus(response.Headers[ProtocolConstants.Headers.CopyStatus]);
        }

    }
}
