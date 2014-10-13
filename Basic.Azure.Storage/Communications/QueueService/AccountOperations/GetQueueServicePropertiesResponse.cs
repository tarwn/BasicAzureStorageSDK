using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.Utility;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Basic.Azure.Storage.Communications.QueueService.AccountOperations
{
    public class GetQueueServicePropertiesResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse, IReceiveDataWithResponse
    {
        public StorageServiceProperties Properties { get; private set; }

        public string RequestId { get; protected set; }

        public string Version { get; protected set; }


        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            RequestId = response.Headers[ProtocolConstants.Headers.RequestId];
            Version = response.Headers[ProtocolConstants.Headers.Version];
        }

        public void ParseResponseBody(Stream responseStream)
        {
            Properties = new StorageServiceProperties();

            using (StreamReader sr = new StreamReader(responseStream))
            {
                var content = sr.ReadToEnd();
                if (content.Length > 0)
                {
                    var xDoc = XDocument.Parse(content);
                    foreach (var topField in xDoc.Root.Elements())
                    {
                        if (topField.Name.LocalName.Equals("Logging", StringComparison.InvariantCultureIgnoreCase))
                        {
                            foreach (var field in topField.Elements())
                            {
                                switch (field.Name.LocalName)
                                {
                                    case "Version":
                                        Properties.Logging.Version = StorageAnalyticsVersionNumber.v1_0;
                                        break;
                                    case "Delete":
                                        Properties.Logging.Delete = field.Value.Equals("true");
                                        break;
                                    case "Read":
                                        Properties.Logging.Read = field.Value.Equals("true");
                                        break;
                                    case "Write":
                                        Properties.Logging.Write = field.Value.Equals("true");
                                        break;
                                    case "RetentionPolicy":
                                        foreach (var retentionField in field.Elements())
                                        {
                                            switch (retentionField.Name.LocalName)
                                            {
                                                case "Enabled":
                                                    Properties.Logging.RetentionPolicyEnabled = retentionField.Value.Equals("true");
                                                    break;
                                                case "Days":
                                                    Properties.Logging.RetentionPolicyNumberOfDays = int.Parse(retentionField.Value);
                                                    break;
                                            }
                                        }
                                        break;
                                }
                            }
                        }
                        else if (topField.Name.LocalName.Equals("Metrics", StringComparison.InvariantCultureIgnoreCase))
                        {
                            foreach (var field in topField.Elements())
                            {
                                switch (field.Name.LocalName)
                                {
                                    case "Version":
                                        Properties.Metrics.Version = StorageAnalyticsVersionNumber.v1_0;
                                        break;
                                    case "Enabled":
                                        Properties.Metrics.Enabled = field.Value.Equals("true");
                                        break;
                                    case "IncludeAPIs":
                                        Properties.Metrics.IncludeAPIs = field.Value.Equals("true");
                                        break;
                                    case "RetentionPolicy":
                                        foreach (var retentionField in field.Elements())
                                        {
                                            switch (retentionField.Name.LocalName)
                                            {
                                                case "Enabled":
                                                    Properties.Metrics.RetentionPolicyEnabled = retentionField.Value.Equals("true");
                                                    break;
                                                case "Days":
                                                    Properties.Metrics.RetentionPolicyNumberOfDays = int.Parse(retentionField.Value);
                                                    break;
                                            }
                                        }
                                        break;
                                }
                            }
                        }
                    }

                    
                }
            }
        }
    }
}
