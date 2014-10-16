using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.BlobService.ContainerOperations
{
    /// <summary>
    /// Get the conatiner's properties, including lease status and metadata
    /// http://msdn.microsoft.com/en-us/library/azure/dd179370.aspx
    /// </summary>
    public class GetContainerPropertiesRequest : RequestBase<GetContainerPropertiesResponse>, ISendAdditionalOptionalHeaders
    {
        private string _containerName;

        public GetContainerPropertiesRequest(StorageAccountSettings settings, string containerName)
            : base(settings)
        {
            _containerName = containerName;
        }

        protected override string HttpMethod { get { return "GET"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.BlobService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.BlobEndpoint);
            builder.AddSegment(_containerName);
            builder.AddParameter(ProtocolConstants.QueryParameters.ResType, ProtocolConstants.QueryValues.ResType.Container);
            return builder;
        }

        public void ApplyAdditionalOptionalHeaders(System.Net.WebRequest request)
        {
        }
    }
}
