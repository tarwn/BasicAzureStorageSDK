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
    /// Sets policies and public access for a container
    /// http://msdn.microsoft.com/en-us/library/azure/dd179391.aspx
    /// </summary>
    public class SetContainerACLRequest : RequestBase<EmptyResponsePayload>, ISendAdditionalOptionalHeaders, ISendDataWithRequest
    {
        private string _containerName;
        private ContainerAccessType _containerAccess;
        private byte[] _content;
        private string _leaseId;

        public SetContainerACLRequest(StorageAccountSettings settings, string containerName, ContainerAccessType containerAccess, List<BlobSignedIdentifier> signedIdentifiers, string leaseId = null)
            : base(settings)
        {
            _containerName = containerName;
            _containerAccess = containerAccess;
            _content = PrepareContent(signedIdentifiers ?? new List<BlobSignedIdentifier>());
            _leaseId = leaseId;
        }

        protected override string HttpMethod { get { return "PUT"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.BlobService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.BlobEndpoint);
            builder.AddSegment(_containerName);
            builder.AddParameter(ProtocolConstants.QueryParameters.ResType, ProtocolConstants.QueryValues.ResType.Container);
            builder.AddParameter(ProtocolConstants.QueryParameters.Comp, ProtocolConstants.QueryValues.ACL);
            return builder;
        }

        public void ApplyAdditionalOptionalHeaders(System.Net.WebRequest request)
        {
            if (_containerAccess == ContainerAccessType.PublicBlob)
            {
                request.Headers.Add(ProtocolConstants.Headers.BlobPublicAccess, ProtocolConstants.HeaderValues.BlobPublicAccess.Blob);
            }
            else if (_containerAccess == ContainerAccessType.PublicContainer)
            {
                request.Headers.Add(ProtocolConstants.Headers.BlobPublicAccess, ProtocolConstants.HeaderValues.BlobPublicAccess.Container);
            }

            if (!string.IsNullOrEmpty(_leaseId))
            {
                request.Headers.Add(ProtocolConstants.Headers.LeaseId, _leaseId);
            }
        }

        private byte[] PrepareContent(List<BlobSignedIdentifier> signedIdentifiers)
        {
            var sb = new StringBuilder("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<SignedIdentifiers>");
            foreach (var identifier in signedIdentifiers)
            {
                sb.AppendFormat("<SignedIdentifier><Id>{0}</Id><AccessPolicy><Start>{1:o}</Start><Expiry>{2:o}</Expiry><Permission>{3}</Permission></AccessPolicy></SignedIdentifier>",
                                identifier.Id,
                                identifier.AccessPolicy.StartTime,
                                identifier.AccessPolicy.Expiry,
                                SharedAccessPermissionParse.ConvertToString(identifier.AccessPolicy.Permission));
            }
            sb.Append("</SignedIdentifiers>");

            var bytes = UTF8Encoding.UTF8.GetBytes(sb.ToString());
            var str = UTF8Encoding.UTF8.GetString(bytes);
            return bytes;
        }

        public byte[] GetContentToSend()
        {
            return _content;
        }

        public int GetContentLength()
        {
            return _content.Length;
        }
    }
}
