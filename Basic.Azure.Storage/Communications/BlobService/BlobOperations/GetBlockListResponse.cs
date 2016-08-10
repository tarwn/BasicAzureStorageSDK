using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using System.Xml.Linq;
using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    public class GetBlockListResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse, IReceiveDataWithResponse
    {
        public virtual DateTime LastModified { get; protected set; }

        public virtual string ETag { get; protected set; }

        public virtual string ContentType { get; protected set; }

        public virtual int BlobContentLength { get; protected set; }

        public virtual DateTime Date { get; protected set; }

        public virtual List<ParsedBlockListBlockId> CommittedBlocks { get; protected set; }

        public virtual List<ParsedBlockListBlockId> UncommittedBlocks { get; protected set; }

        public void ParseHeaders(HttpWebResponse response)
        {
            //TODO: determine what we want to do about potential missing headers and date parsing errors

            LastModified = Parsers.ParseDateHeader(response.Headers[ProtocolConstants.Headers.LastModified]);

            // ETag will only be present if the blob has committed blocks
            var rawETag = response.Headers[ProtocolConstants.Headers.ETag];
            ETag = (string.IsNullOrEmpty(rawETag) ? "" : rawETag.Trim('"'));

            Date = Parsers.ParseDateHeader(response.Headers[ProtocolConstants.Headers.OperationDate]);
            ContentType = response.Headers[ProtocolConstants.Headers.ContentType];

            // ContentLength will be null if there are no committed blocks, so we'll use Convert.ToInt
            BlobContentLength = Convert.ToInt32(response.Headers[ProtocolConstants.Headers.BlobContentLength]);
        }

        public async Task ParseResponseBodyAsync(Stream responseStream, string contentType)
        {
            using (var sr = new StreamReader(responseStream))
            {
                var content = await sr.ReadToEndAsync();
                if (content.Length > 0)
                {
                    var xDoc = XDocument.Parse(content);
                    var parsedBlockLists = Parsers.ParseAllXmlBlockLists(xDoc.Root);

                    CommittedBlocks = parsedBlockLists[GetBlockListListType.Committed];
                    UncommittedBlocks = parsedBlockLists[GetBlockListListType.Uncommitted];
                }
            }
        }
    }
}
