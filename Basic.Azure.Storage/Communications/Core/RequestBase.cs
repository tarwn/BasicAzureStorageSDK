using Microsoft.Practices.TransientFaultHandling;
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
            RetryPolicy = new RetryPolicy<ExceptionRetryStrategy>(RetryStrategy.DefaultExponential);
        }

        public RetryPolicy RetryPolicy { get; set; }

        protected StorageAccountSettings Settings { get { return _settings; } }

        protected abstract RequestUriBuilder GetUriBase();
        protected abstract string HttpMethod { get; }
        protected abstract AuthenticationMethod AuthenticationMethod { get; }
        protected abstract void ApplyRequiredHeaders(WebRequest request);
        protected abstract void ApplyOptionalHeaders(WebRequest request);

        public Response<TPayload> Execute()
        {
            return ExecuteAsync().Result;
        }
        
        public Task<Response<TPayload>> ExecuteAsync()
        { 
            // create web request
            var requestUri = GetUriBase();
            var request = WebRequest.Create(requestUri.GetUri());

            request.Method = HttpMethod;
            request.ContentLength = 0;

            {
                // hacky workaround area - can't test against HttpWebRequest and UserAgent property requires you use the named property
                if (request is HttpWebRequest)
                    ((HttpWebRequest)request).UserAgent = "Basic.Azure.Storage/1.0.0";
            }

            // apply required headers
            request.Headers.Add(ProtocolConstants.Headers.StorageVersion, TargetStorageVersion);
            request.Headers.Add(ProtocolConstants.Headers.Date, DateTime.UtcNow.ToString("R", CultureInfo.InvariantCulture));
            ApplyRequiredHeaders(request);

            // apply optional headers
            ApplyOptionalHeaders(request);

            // apply authorization header
            ApplyAuthorizationHeader(AuthenticationMethod, request, requestUri.GetParameters(), _settings);

            // send web request
            return SendRequestWithRetryAsync(request);
        }

        private static void ApplyAuthorizationHeader(AuthenticationMethod authMethod, WebRequest request, Dictionary<string,string> queryStringParameters, StorageAccountSettings settings)
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

        private Task<Response<TPayload>> SendRequestWithRetryAsync(WebRequest request) 
        {
            int numberOfAttempts = 0;
            return RetryPolicy.ExecuteAsync<Response<TPayload>>(() =>
            {
                numberOfAttempts++;
                var responseTask = SendRequestAsync(request);
                responseTask.ContinueWith((t) =>
                {
                    if (!t.IsFaulted)
                        t.Result.NumberOfAttempts = numberOfAttempts;
                });
                return responseTask;
            })
            .ContinueWith<Response<TPayload>>((t) =>
            {
                if (t.IsFaulted && numberOfAttempts > 1)
                    throw new RetriedException(t.Exception, numberOfAttempts);
                else
                    return t.Result;
            });
        }

        private Task<Response<TPayload>> SendRequestAsync(WebRequest request)
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
