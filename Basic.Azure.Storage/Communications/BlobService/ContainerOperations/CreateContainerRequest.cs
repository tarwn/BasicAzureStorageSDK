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
    /// Create a container with the given name
    /// http://msdn.microsoft.com/en-us/library/windowsazure/dd179468.aspx
    /// </summary>
    public class CreateContainerRequest : RequestBase<EmptyResponsePayload>, ISendAdditionalOptionalHeaders
    {
        private string _containerName;
        private ContainerAccessType _containerAccessType;

        public CreateContainerRequest(StorageAccountSettings settings, string containerName, ContainerAccessType containerAccessType)
            : base(settings)
        {
            _containerName = containerName;
            _containerAccessType = containerAccessType;
        }

        protected override string HttpMethod { get { return "PUT"; } }

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
            if (_containerAccessType == ContainerAccessType.PublicContainer)
                request.Headers.Add(ProtocolConstants.Headers.BlobPublicAccess, ProtocolConstants.HeaderValues.BlobPublicAccess.Container);
            else if (_containerAccessType == ContainerAccessType.PublicBlob)
                request.Headers.Add(ProtocolConstants.Headers.BlobPublicAccess, ProtocolConstants.HeaderValues.BlobPublicAccess.Blob);
        }
    }
}
