using Basic.Azure.Storage.Communications.Core;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Threading.Tasks;
using Basic.Azure.Storage.Communications.Core.Interfaces;

namespace Basic.Azure.Storage.Communications.BlobService.BlobOperations
{
    public class GetBlobResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse, IReceiveDataWithResponse
    {
        private Stream _stream;

        public DateTime Date { get; protected set; }

        public string ETag { get; protected set; }

        public DateTime LastModified { get; protected set; }

        public ReadOnlyDictionary<string, string> Metadata { get; protected set; }

        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            //TODO: determine what we want to do about potential missing headers and date parsing errors

            ETag = response.Headers[ProtocolConstants.Headers.ETag].Trim(new char[] { '"' });
            Date = ParseDate(response.Headers[ProtocolConstants.Headers.OperationDate]);
            LastModified = ParseDate(response.Headers[ProtocolConstants.Headers.LastModified]);

            var metadata = new Dictionary<string, string>();
            foreach (var headerKey in response.Headers.AllKeys)
            {
                if (headerKey.StartsWith(ProtocolConstants.Headers.MetaDataPrefix, StringComparison.InvariantCultureIgnoreCase))
                {
                    metadata[headerKey.Substring(ProtocolConstants.Headers.MetaDataPrefix.Length)] = response.Headers[headerKey];
                }
            }
            Metadata = new ReadOnlyDictionary<string, string>(metadata);
        }

        private DateTime ParseDate(string headerValue)
        {
            DateTime dateValue;
            DateTime.TryParse(headerValue, out dateValue);
            return dateValue;
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
