using Basic.Azure.Storage.Communications.Core;
using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Threading.Tasks;
using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    public class GetBlobResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse, IReceiveDataWithResponse
    {
        private Stream _stream;

        public virtual DateTime Date { get; protected set; }

        public virtual string ETag { get; protected set; }

        public virtual DateTime LastModified { get; protected set; }

        public virtual ReadOnlyDictionary<string, string> Metadata { get; protected set; }

        public virtual BlobType BlobType { get; protected set; }

        public virtual DateTime? CopyCompletionTime { get; protected set; }

        public virtual string CopyStatusDescription { get; protected set; }

        public virtual string CopyId { get; protected set; }

        public virtual BlobCopyProgress CopyProgress { get; protected set; }

        public virtual string CopySource { get; protected set; }

        public virtual CopyStatus? CopyStatus { get; protected set; }

        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            //TODO: determine what we want to do about potential missing headers and date parsing errors

            ETag = response.Headers[ProtocolConstants.Headers.ETag].Trim(new char[] { '"' });
            Date = DateParse.ParseHeader(response.Headers[ProtocolConstants.Headers.OperationDate]);
            LastModified = DateParse.ParseHeader(response.Headers[ProtocolConstants.Headers.LastModified]);

            Metadata = MetadataParse.ParseMetadata(response);

            BlobType = BlobTypeParse.ParseBlobType(response.Headers[ProtocolConstants.Headers.BlobType]);

            CopyCompletionTime = CopyHeadersParse.ParseCopyCompletionTime(response);
            CopyStatusDescription = CopyHeadersParse.ParseCopyStatusDescription(response);
            CopyId = CopyHeadersParse.ParseCopyId(response);
            CopyProgress = CopyHeadersParse.ParseCopyProgress(response);
            CopySource = CopyHeadersParse.ParseCopySource(response);
            CopyStatus = CopyHeadersParse.ParseCopyStatus(response);
        }

        public virtual async Task ParseResponseBodyAsync(System.IO.Stream responseStream)
        {
            _stream = responseStream;
        }

        public virtual byte[] GetDataBytes()
        {
            using (_stream)
            {
                using (var ms = new MemoryStream())
                {
                    _stream.CopyTo(ms);
                    return ms.ToArray();
                }
            }
        }

        public virtual Stream GetDataStream()
        {
            return _stream;
        }
    }
}
