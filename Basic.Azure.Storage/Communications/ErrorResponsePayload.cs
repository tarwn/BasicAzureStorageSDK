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
        public string ErrorCode { get; set; }
        public string ErrorMessage { get; set; }

        public void ParseResponseBody(Stream responseStream)
        {
            using (StreamReader sr = new StreamReader(responseStream))
            {
                var content = sr.ReadToEnd();
                if (content.Length > 0)
                {
                    var xDoc = XDocument.Parse(content);
                    try
                    {
                        ErrorCode = xDoc.Element("Error").Element("Code").Value;
                    }
                    catch
                    {
                        ErrorCode = "CodeNotProvided";
                    }

                    try
                    {
                        ErrorMessage = xDoc.Element("Error").Element("Message").Value;
                    }
                    catch
                    {
                        ErrorMessage = "CodeNotProvided";
                    }
                }
            }
        }
    }
}
