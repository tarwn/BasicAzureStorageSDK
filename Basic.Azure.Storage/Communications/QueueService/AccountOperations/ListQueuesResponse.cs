﻿using Basic.Azure.Storage.Communications.Common;
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
    public class ListQueuesResponse : IResponsePayload, IReceiveAdditionalHeadersWithResponse, IReceiveDataWithResponse
    {

        public virtual string Prefix { get; protected set; }
        public virtual string Marker { get; protected set; }
        public virtual int MaxResults { get; protected set; }
        public virtual ReadOnlyCollection<Queue> Queues { get; protected set; }


        public virtual string RequestId { get; protected set; }

        public virtual string Version { get; protected set; }

        public virtual DateTime Date { get; protected set; }

        private DateTime ParseDate(string headerValue)
        {
            DateTime dateValue;
            DateTime.TryParse(headerValue, out dateValue);
            return dateValue;
        }

        public void ParseHeaders(System.Net.HttpWebResponse response)
        {
            //TODO: determine what we want to do about potential missing headers and date parsing errors

            RequestId = response.Headers[ProtocolConstants.Headers.RequestId];
            Version = response.Headers[ProtocolConstants.Headers.Version];
            Date = ParseDate(response.Headers[ProtocolConstants.Headers.OperationDate]);
        }

        public async Task ParseResponseBodyAsync(Stream responseStream, string contentType)
        {
            using (StreamReader sr = new StreamReader(responseStream))
            {
                var content = await sr.ReadToEndAsync();
                if (content.Length > 0)
                {
                    var xDoc = XDocument.Parse(content);
                    foreach (var field in xDoc.Root.Elements().Where(e => !e.Name.LocalName.Equals("QueueMessage")))
                    {
                        switch (field.Name.LocalName)
                        {
                            case "Prefix":
                                Prefix = field.Value;
                                break;
                            case "Marker":
                                Marker = field.Value;
                                break;
                            case "MaxResults":
                                MaxResults = int.Parse(field.Value);
                                break;
                        }
                    }

                    var receivedQueues = new List<Queue>();

                    foreach (var queue in xDoc.Root.Elements().Where(e => e.Name.LocalName.Equals("Queues")).Elements())
                    {
                        var receivedQueue = new Queue();
                        foreach (var field in queue.Elements())
                        {
                            switch (field.Name.LocalName)
                            {
                                case "Name":
                                    receivedQueue.Name = field.Value;
                                    break;
                                case "Metadata":
                                    foreach (var metadataElement in field.Elements())
                                    {
                                        receivedQueue.Metadata.Add(metadataElement.Name.LocalName, metadataElement.Value);
                                    }
                                    break;
                            }
                        }
                        receivedQueues.Add(receivedQueue);
                    }

                    Queues = new ReadOnlyCollection<Queue>(receivedQueues);
                }
            }
        }
    }
}
