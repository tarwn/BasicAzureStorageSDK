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

namespace Basic.Azure.Storage.Communications.QueueService.MessageOperations
{
    public class PeekMessagesResponse : IResponsePayload, IReceiveDataWithResponse
    {
        public PeekMessagesResponse()
        {
            Messages = new ReadOnlyCollection<QueueMessage>(new List<QueueMessage>());
        }

        public ReadOnlyCollection<QueueMessage> Messages { get; protected set; }

        public string RequestId { get; protected set; }

        public string Version { get; protected set; }

        public DateTime Date { get; protected set; }

        private DateTime ParseDate(string headerValue)
        {
            DateTime dateValue;
            DateTime.TryParse(headerValue, out dateValue);
            return dateValue;
        }

        public void ParseResponseBody(System.IO.Stream responseStream)
        {
            using (StreamReader sr = new StreamReader(responseStream))
            {
                var content = sr.ReadToEnd();
                if (content.Length > 0)
                {
                    var xDoc = XDocument.Parse(content);
                    var receivedMessages = new List<QueueMessage>();
                    foreach (var message in xDoc.Root.Elements().Where(e => e.Name.LocalName.Equals("QueueMessage")))
                    {
                        var receivedMessage = new QueueMessage();
                        foreach (var field in message.Elements())
                        { 
                            switch(field.Name.LocalName){
                                case "MessageId":
                                    receivedMessage.MessageId = field.Value;
                                    break;
                                case "InsertionTime":
                                    receivedMessage.InsertionTime = DateTime.Parse(field.Value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
                                    break;
                                case "ExpirationTime":
                                    receivedMessage.ExpirationTime = DateTime.Parse(field.Value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
                                    break;
                                case "PopReceipt":
                                    receivedMessage.PopReceipt = field.Value;
                                    break;
                                case "TimeNextVisible":
                                    receivedMessage.TimeNextVisible = DateTime.Parse(field.Value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
                                    break;
                                case "DequeueCount":
                                    receivedMessage.DequeueCount = int.Parse(field.Value);
                                    break;
                                case "MessageText":
                                    receivedMessage.MessageText = field.Value;
                                    break;
                            }
                        }
                        receivedMessages.Add(receivedMessage);
                    }

                    Messages = new ReadOnlyCollection<QueueMessage>(receivedMessages);
                }
            }
        }
    }
}
