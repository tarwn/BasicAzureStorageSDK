using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;
using Basic.Azure.Storage.Communications.Utility;

namespace Basic.Azure.Storage.Communications.BlobService.ContainerOperations
{
    public class LeaseContainerRenewResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse
    {
        public virtual DateTime Date { get; protected set; }

        // 2013-0815 forward - public string ETag { get; protected set; }

        public virtual DateTime LastModified { get; protected set; }

        public virtual string LeaseId { get; protected set; }

        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            //TODO: determine what we want to do about potential missing headers and date parsing errors

            // 2013-0815 forward - ETag = response.Headers[ProtocolConstants.Headers.ETag].Trim(new char[] { '"' });
            Date = DateParse.ParseHeader(response.Headers[ProtocolConstants.Headers.OperationDate]);
            LastModified = DateParse.ParseHeader(response.Headers[ProtocolConstants.Headers.LastModified]);

            LeaseId = response.Headers[ProtocolConstants.Headers.LeaseId];
        }
    }
}
