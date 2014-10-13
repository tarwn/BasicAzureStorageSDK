using Basic.Azure.Storage.Communications.Common;
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
    /// Gets the Logging and Metrics properties for the Queue Service
    /// http://msdn.microsoft.com/en-us/library/azure/hh452243.aspx
    /// </summary>
    public class GetQueueServicePropertiesRequest : RequestBase<GetQueueServicePropertiesResponse>
    {

        public GetQueueServicePropertiesRequest(StorageAccountSettings settings)
            : base(settings)
        {

        }

        protected override string HttpMethod { get { return "GET"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.QueueService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.QueueEndpoint);

            builder.AddParameter(ProtocolConstants.QueryParameters.ResType, ProtocolConstants.QueryValues.ResType.Service);
            builder.AddParameter(ProtocolConstants.QueryParameters.Comp, ProtocolConstants.QueryValues.Comp.Properties);

            return builder;
        }

    }
}
