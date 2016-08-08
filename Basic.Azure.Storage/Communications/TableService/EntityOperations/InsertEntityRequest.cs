using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.TableService.Interfaces;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.TableService.EntityOperations
{
    public class InsertEntityRequest<TEntity> : RequestBase<EmptyResponsePayload>,
                                                ISendAdditionalRequiredHeaders,
                                                ISendAdditionalOptionalHeaders,
                                                ISendDataWithRequest
        where TEntity : ITableEntity, new()
    {
        private string _tableName;
        private string _content;
        private byte[] _contentData;
        private MetadataPreference _entityResponseEcho;

        public InsertEntityRequest(StorageAccountSettings settings, string tableName, TEntity entity)
            : base(settings)
        {
            _tableName = tableName;
            // emulator prior to 2.2.1 preview does not support json
            _content = JsonConvert.SerializeObject(entity);
            _contentData = UTF8Encoding.UTF8.GetBytes(_content);
            _entityResponseEcho = MetadataPreference.ReturnNoContent;
        }

        protected override string HttpMethod { get { return "POST"; } }

        protected override StorageServiceType ServiceType { get { return StorageServiceType.TableService; } }

        protected override RequestUriBuilder GetUriBase()
        {
            var builder = new RequestUriBuilder(Settings.TableEndpoint);
            builder.AddSegment(_tableName);
            return builder;
        }

        public void ApplyAdditionalRequiredHeaders(System.Net.WebRequest request)
        {
            request.ContentType = "application/json;charset=utf-8";

            if (request is HttpWebRequest)
            {
                ((HttpWebRequest)request).Accept = "application/json";
            }
            else
            {
                request.Headers.Add("Accept", "application/json");
            }
        }

        public void ApplyAdditionalOptionalHeaders(WebRequest request)
        {
            request.Headers.Add("Prefer", ProtocolConstants.HeaderValues.TableMetadataPreference.GetValue(_entityResponseEcho));
        }

        public byte[] GetContentToSend()
        {
            return _contentData;
        }

        public int GetContentLength()
        {
            return _contentData.Length;
        }
    }
}
