using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Core
{
    public abstract class RequestBase<TPayload>
        where TPayload : IResponsePayload, new()
    {
        public const string TargetStorageVersion = "2012-02-12";

        private StorageAccountSettings _settings;

        public RequestBase(StorageAccountSettings settings)
        {
            _settings = settings;
        }

        protected StorageAccountSettings Settings { get { return _settings; } }

        protected abstract RequestUriBuilder GetUriBase();
        protected abstract string HttpMethod { get; }
        protected abstract AuthenticationMethod AuthenticationMethod { get; }
        protected abstract void ApplyRequiredHeaders(HttpWebRequest request);
        protected abstract void ApplyOptionalHeaders(HttpWebRequest request);

        public Response<TPayload> Execute()
        {
            // create web request
            var requestUri = GetUriBase();
            var request = (HttpWebRequest)WebRequest.Create(requestUri.GetUri());

            request.Method = HttpMethod;
            request.UserAgent = "BasicAzureStorage/1.0.0";
            request.ContentLength = 0;

            // apply required headers
            request.Headers.Add(ProtocolConstants.Headers.StorageVersion, TargetStorageVersion);
            request.Headers.Add(ProtocolConstants.Headers.Date, DateTime.UtcNow.ToString("R", CultureInfo.InvariantCulture));
            ApplyRequiredHeaders(request);

            // apply optional headers
            ApplyOptionalHeaders(request);

            // apply authorization header
            ApplyAuthorizationHeader(AuthenticationMethod, request, requestUri.GetParameters(), _settings);

            // send web request
            return SendRequestAsync(request).Result;
        }

        private static void ApplyAuthorizationHeader(AuthenticationMethod authMethod, HttpWebRequest request, Dictionary<string,string> queryStringParameters, StorageAccountSettings settings)
        {
            switch (authMethod)
            { 
                case AuthenticationMethod.SharedKeyForBlobAndQueueServices: 
                    string signedString = SignedAuthorization.GenerateSharedKeySignatureString(request, queryStringParameters, settings);    
                    request.Headers.Add(ProtocolConstants.Headers.Authorization, String.Format("SharedKey {0}:{1}", settings.AccountName, signedString));
                    break;
                default:
                    throw new NotImplementedException("Specified authentication type is not supported yet");
            }
        }

        private Task<Response<TPayload>> SendRequestAsync(HttpWebRequest request)
        {
            var task = Task.Factory.FromAsync(
                request.BeginGetResponse,
                asyncResult => request.EndGetResponse(asyncResult),
                null);

            return task.ContinueWith(t => ReceiveResponse((HttpWebResponse)t.Result));
        }

        private Response<TPayload> ReceiveResponse(HttpWebResponse httpWebResponse)
        {
            var response = new Response<TPayload>(httpWebResponse);
            return response;
        }

    }
}
