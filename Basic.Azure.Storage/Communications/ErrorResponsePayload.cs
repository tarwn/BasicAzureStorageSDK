using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Basic.Azure.Storage.Communications
{
    public class ErrorResponsePayload : IResponsePayload, IReceiveDataWithResponse
    {
        public ErrorResponsePayload()
        {
            Details = new Dictionary<string, string>();
        }

        public string ErrorCode { get; set; }
        public string ErrorMessage { get; set; }
        public Dictionary<string,string> Details { get; set; }

        public async Task ParseResponseBodyAsync(Stream responseStream)
        {
            using (StreamReader sr = new StreamReader(responseStream))
            {
                               
                var content = await sr.ReadToEndAsync();
                if (content.Length > 0)
                {
                    ErrorCode = "CodeNotProvided";
                    ErrorMessage = "Message Not Provided";

                    var xDoc = XDocument.Parse(content);
                    foreach (var element in xDoc.Root.Elements())
                    {
                        switch (element.Name.LocalName.ToLowerInvariant())
                        { 
                            case "code":
                                ErrorCode = element.Value;
                                break;
                            case "message":
                                ErrorMessage = element.Value;
                                break;
                            default:
                                Details.Add(element.Name.LocalName, element.Value);
                                break;
                        }
                    }
                }
            }
        }
    }
}
