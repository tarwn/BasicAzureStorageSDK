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
            AppendMetricsString(sb, "HourMetrics", properties.HourMetrics);
            AppendMetricsString(sb, "MinuteMetrics", properties.MinuteMetrics);
            
            // Cors
            sb.Append("<Cors>");
            foreach (var rule in properties.Cors)
            {
                sb.AppendFormat("<CorsRule><AllowedOrigins>{0}</AllowedOrigins><AllowedMethods>{1}</AllowedMethods><MaxAgeInSeconds>{2}</MaxAgeInSeconds><ExposedHeaders>{3}</ExposedHeaders><AllowedHeaders>{4}</AllowedHeaders></CorsRule>",
                                 string.Join(",",rule.AllowedOrigins),
                                 string.Join(",",rule.AllowedMethods),
                                 rule.MaxAgeInSeconds,
                                 string.Join(",",rule.ExposedHeaders),
                                 string.Join(",",rule.AllowedHeaders));
            }
            sb.Append("</Cors>");

            sb.Append("</StorageServiceProperties>");

            return Encoding.ASCII.GetBytes(sb.ToString());
        }

        private static void AppendMetricsString(StringBuilder sb, string metricsName, StorageServiceMetricsProperties metrics)
        {
            sb.AppendFormat("<{0}><Version>{1}</Version><Enabled>{2}</Enabled>",
                metricsName,
                "1.0",
                metrics.Enabled ? "true" : "false");

            if (metrics.Enabled)
                sb.AppendFormat("<IncludeAPIs>{0}</IncludeAPIs>", metrics.IncludeAPIs ? "true" : "false");

            if (metrics.RetentionPolicyEnabled)
                sb.AppendFormat("<RetentionPolicy><Enabled>true</Enabled><Days>{0}</Days></RetentionPolicy></{1}>", metrics.RetentionPolicyNumberOfDays, metricsName);
            else
                sb.AppendFormat("<RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></{0}>", metricsName);
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
