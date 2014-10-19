using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.BlobService.ContainerOperations
{
    /// <summary>
    /// List all the blobs in a container, with optional prefix, delimiter, size, data to be returned, and continuation markers
    /// http://msdn.microsoft.com/en-us/library/azure/dd135734.aspx
    /// </summary>
    public class ListBlobsRequest : RequestBase<ListBlobsResponse>
    {
        private string _containerName;
        private string _prefix;
        private string _delimiter;
        private string _marker;
        private int _maxResults;
        private ListBlobsInclude? _include;

        public ListBlobsRequest(StorageAccountSettings settings, string containerName, string prefix = "", string delimiter = "", string marker = "", int maxResults = 5000, ListBlobsInclude? include = null)
            : base(settings)
        {
            _containerName = containerName;
            _prefix = prefix;
            _delimiter = delimiter;
            _marker = marker;
            _maxResults = maxResults;
            _include = include;
        }

        protected override string HttpMethod { get { return "GET"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.BlobService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.BlobEndpoint);
            
            builder.AddSegment(_containerName);
            builder.AddParameter(ProtocolConstants.QueryParameters.ResType, ProtocolConstants.QueryValues.ResType.Container);
            builder.AddParameter(ProtocolConstants.QueryParameters.Comp, ProtocolConstants.QueryValues.List);

            if (!string.IsNullOrEmpty(_prefix))
            {
                builder.AddParameter(ProtocolConstants.QueryParameters.Prefix, _prefix);
            }
            if (!string.IsNullOrEmpty(_delimiter))
            {
                builder.AddParameter(ProtocolConstants.QueryParameters.Delimiter, _delimiter);
            }
            if (!string.IsNullOrEmpty(_marker))
            {
                builder.AddParameter(ProtocolConstants.QueryParameters.Marker, _marker);
            }
            builder.AddParameter(ProtocolConstants.QueryParameters.MaxResults, _maxResults.ToString());
            if (_include.HasValue)
            {
                builder.AddParameter(ProtocolConstants.QueryParameters.Include, BlobListIncludeParse.ConvertToString(_include.Value));
            }

            return builder;
        }

    }
}
