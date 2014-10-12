using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.QueueService.AccountOperations
{
    /// <summary>
    /// Retrieves an alphabetical list of the queus that match the supplied prefix and maximum count, optionally returning a continuation marker
    /// http://msdn.microsoft.com/en-us/library/azure/dd179466.aspx
    /// </summary>
    public class ListQueuesRequest : RequestBase<ListQueuesResponse>
    {
        private string _prefix;
        private int _maxResults;
        private string _marker;
        private bool _includeMetadata;

        public ListQueuesRequest(StorageAccountSettings settings, string prefix = "", int maxResults = 5000, string marker = null, bool includeMetadata = false)
            : base(settings)
        {
            _prefix = prefix;
            _maxResults = maxResults;
            _marker = marker;
            _includeMetadata = includeMetadata;
        }

        protected override string HttpMethod { get { return "GET"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.QueueService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.QueueEndpoint);

            builder.AddParameter(ProtocolConstants.QueryParameters.Comp, ProtocolConstants.QueryValues.List);

            builder.AddParameter(ProtocolConstants.QueryParameters.Prefix, _prefix);
            builder.AddParameter(ProtocolConstants.QueryParameters.MaxResults, _maxResults.ToString());
            
            if(!string.IsNullOrEmpty(_marker))
                builder.AddParameter(ProtocolConstants.QueryParameters.Marker, _marker);

            if(_includeMetadata)
                builder.AddParameter(ProtocolConstants.QueryParameters.Include, ProtocolConstants.QueryValues.Metadata);

            return builder;
        }


    }
}
