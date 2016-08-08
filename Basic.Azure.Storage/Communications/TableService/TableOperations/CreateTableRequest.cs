using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.TableService.TableOperations
{
    public class CreateTableRequest : RequestBase<CreateTableResponse>,
                                      ISendAdditionalRequiredHeaders,
                                      ISendAdditionalOptionalHeaders,
                                      ISendDataWithRequest
    {
        private string _tableName;
        private byte[] _content;
        private MetadataPreference? _metadataPreference;

        public CreateTableRequest(StorageAccountSettings settings, string tableName, MetadataPreference? metadataPreference = null)
            : base(settings)
        {
            _tableName = tableName;
            // emulator prior to 2.2.1 preview does not support json
            _content = UTF8Encoding.UTF8.GetBytes(GetAtomContent(_tableName));
            _metadataPreference = metadataPreference;
        }

        protected override string HttpMethod { get { return "POST"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.TableService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.TableEndpoint);
            builder.AddSegment("Tables");
            return builder;
        }

        public void ApplyAdditionalRequiredHeaders(System.Net.WebRequest request)
        {
            request.ContentType = "application/atom+xml;type=entry;charset=utf-8";
            
            if (request is HttpWebRequest)
            {
                ((HttpWebRequest)request).Accept = "application/atom+xml,application/xml";
            }
            else
            {
                request.Headers.Add("Accept", "application/atom+xml,application/xml");
            }

            // these are not documented as required, but older emulator fails without them
            //request.Headers.Add("DataServiceVersion", "2.0;");
            //request.Headers.Add("MaxDataServiceVersion", "2.0;NetFx");
        }

        public void ApplyAdditionalOptionalHeaders(WebRequest request)
        {
            if(_metadataPreference.HasValue)
                request.Headers.Add("Prefer", ProtocolConstants.HeaderValues.TableMetadataPreference.GetValue(_metadataPreference.Value));
        }
        
        public byte[] GetContentToSend()
        {
            return _content;
        }

        public int GetContentLength()
        {
            return _content.Length;
        }

        private static string GetJsonContent(string tableName)
        {
            return "{ \"TableName\": \"" + tableName + "\" }";
        }

        private static string GetAtomContent(string tableName)
        { 
            string s = String.Format("<?xml version=\"1.0\" encoding=\"utf-8\"?><entry xmlns=\"http://www.w3.org/2005/Atom\" xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\"><id /><title /><updated>{0}</updated><author><name /></author><content type=\"application/xml\"><m:properties><d:TableName>{1}</d:TableName></m:properties></content></entry>",           
                DateTime.UtcNow.ToString("s", System.Globalization.CultureInfo.InvariantCulture) + "Z", tableName);
            return s;
        }
    }
}
