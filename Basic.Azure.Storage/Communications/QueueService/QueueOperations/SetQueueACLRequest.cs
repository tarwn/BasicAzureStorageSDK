using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.QueueService.QueueOperations
{
    /// <summary>
    /// Retrieve metadata for queue, including approximate message count
    /// http://msdn.microsoft.com/en-us/library/azure/dd179384.aspx
    /// </summary>
    public class SetQueueACLRequest : RequestBase<GetQueueACLResponse>, ISendDataWithRequest
    {
        private string _queueName;
        private byte[] _content;

        public SetQueueACLRequest(StorageAccountSettings settings, string queueName, List<SignedIdentifier> signedIdentifiers)
            : base(settings)
        {
            _queueName = queueName;
            _content = PrepareContent(signedIdentifiers);
        }

        protected override string HttpMethod { get { return "PUT"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.QueueService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.QueueEndpoint);
            builder.AddSegment(_queueName);
            builder.AddParameter(ProtocolConstants.QueryParameters.Comp, ProtocolConstants.QueryValues.ACL);
            return builder;
        }

        private byte[] PrepareContent(List<SignedIdentifier> signedIdentifiers)
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

            return UTF8Encoding.UTF8.GetBytes(sb.ToString());
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
