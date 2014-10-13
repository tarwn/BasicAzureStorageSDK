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
    /// Sets the Logging and Metrics properties for the Queue Service
    /// http://msdn.microsoft.com/en-us/library/azure/hh452232.aspx
    /// </summary>
    public class SetQueueServicePropertiesRequest : RequestBase<EmptyResponsePayload>, ISendDataWithRequest
    {
        StorageServiceProperties _properties;
        private byte[] _content;

        public SetQueueServicePropertiesRequest(StorageAccountSettings settings, StorageServiceProperties properties)
            : base(settings)
        {
            _properties = properties;

            _content = PrepareContent(properties);
        }

        protected override string HttpMethod { get { return "PUT"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.QueueService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.QueueEndpoint);

            builder.AddParameter(ProtocolConstants.QueryParameters.ResType, ProtocolConstants.QueryValues.ResType.Service);
            builder.AddParameter(ProtocolConstants.QueryParameters.Comp, ProtocolConstants.QueryValues.Comp.Properties);

            return builder;
        }

        private static byte[] PrepareContent(StorageServiceProperties properties)
        {
            var sb = new StringBuilder("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<StorageServiceProperties>");

            // Logging properties
            sb.AppendFormat("<Logging><Version>{0}</Version><Delete>{1}</Delete><Read>{2}</Read><Write>{3}</Write>",
                "1.0",
                properties.Logging.Delete ? "true" : "false",
                properties.Logging.Read ? "true" : "false",
                properties.Logging.Write ? "true" : "false");

            if(properties.Logging.RetentionPolicyEnabled)
                sb.AppendFormat("<RetentionPolicy><Enabled>true</Enabled><Days>{0}</Days></RetentionPolicy></Logging>", properties.Logging.RetentionPolicyNumberOfDays);
            else
                sb.Append("<RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></Logging>");

            // Metrics properties
            sb.AppendFormat("<Metrics><Version>{0}</Version><Enabled>{1}</Enabled>",
                "1.0",
                properties.Metrics.Enabled ? "true" : "false");

            if (properties.Metrics.Enabled)
                sb.AppendFormat("<IncludeAPIs>{0}</IncludeAPIs>", properties.Metrics.IncludeAPIs ? "true" : "false");

            if (properties.Metrics.RetentionPolicyEnabled)
                sb.AppendFormat("<RetentionPolicy><Enabled>true</Enabled><Days>{0}</Days></RetentionPolicy></Metrics>", properties.Metrics.RetentionPolicyNumberOfDays);
            else
                sb.Append("<RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></Metrics>");

            sb.Append("</StorageServiceProperties>");

            return Encoding.ASCII.GetBytes(sb.ToString());
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
