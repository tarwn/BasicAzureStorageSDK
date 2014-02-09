using Basic.Azure.Storage.Communications.ServiceExceptions;
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
        protected abstract StorageServiceType ServiceType { get; }
        protected abstract void ApplyRequiredHeaders(WebRequest request);
        protected abstract void ApplyOptionalHeaders(WebRequest request);

        public Response<TPayload> Execute()
        {
            try
            {
                return ExecuteAsync().Result;
            }
            catch (AggregateException ae)
            {
                throw ae.Flatten().InnerExceptions.First();
            }
        }

        public Task<Response<TPayload>> ExecuteAsync()
        {
            // create web request
            var requestUri = GetUriBase();
            var request = WebRequest.Create(requestUri.GetUri());

            request.Method = HttpMethod;
            request.ContentLength = 0;

            {
                // hacky workaround area 
                //  - Can't test against HttpWebRequest because the ctor isn't accessible
                //  - Can't set UserAgent as raw header becauseHttpWebRequest won't allow you to not uset the named property
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
            ApplyAuthorizationHeader(ServiceType, request, requestUri.GetParameters(), _settings);

            // send web request
            return SendRequestWithRetryAsync(request);
        }

        private static void ApplyAuthorizationHeader(StorageServiceType serviceType, WebRequest request, Dictionary<string, string> queryStringParameters, StorageAccountSettings settings)
        {
            switch (serviceType)
            {
                case StorageServiceType.QueueService:
                    string queueSignedString = SignedAuthorization.GenerateSharedKeySignatureString(request, queryStringParameters, settings);
                    request.Headers.Add(ProtocolConstants.Headers.Authorization, String.Format("SharedKey {0}:{1}", settings.AccountName, queueSignedString));
                    break;
                case StorageServiceType.BlobService:
                    string blobSignedString = SignedAuthorization.GenerateSharedKeySignatureString(request, queryStringParameters, settings);
                    request.Headers.Add(ProtocolConstants.Headers.Authorization, String.Format("SharedKey {0}:{1}", settings.AccountName, blobSignedString));
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
                var continuedTask = responseTask.ContinueWith<Response<TPayload>>((t) =>
                {
                    if (!t.IsFaulted)
                    {
                        t.Result.NumberOfAttempts = numberOfAttempts;
                        return t.Result;
                    }
                    else
                    {
                        throw GetAzureExceptionFor(t.Exception);
                    }
                });
                return continuedTask;
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

        private Exception GetAzureExceptionFor(Exception exception)
        {
            if (exception is AggregateException)
            {
                var aggregateException = ((AggregateException)exception).Flatten();

                // see if we can find a webexception
                if (aggregateException.InnerExceptions.Any(ie => ie is WebException))
                {
                    var wexc = aggregateException.InnerExceptions.OfType<WebException>().First();
                    if (wexc.Response != null && typeof(HttpWebResponse).IsAssignableFrom(wexc.Response.GetType()))
                    {
                        var response = new Response<ErrorResponsePayload>((HttpWebResponse)wexc.Response);
                        return GetAzureExceptionFor(response, wexc);
                    }
                    else
                    {
                        return new UnidentifiedAzureException(wexc);
                    }
                }
                else if (aggregateException.InnerExceptions.Count == 1)
                {

                    throw new GeneralExceptionDuringAzureOperationException("Azure service request received an exception without a valid Http Response to parse", aggregateException.InnerExceptions.Single());
                }
                else
                {
                    throw new GeneralExceptionDuringAzureOperationException("Azure service request received exceptions without a valid Http Response to parse", aggregateException.InnerExceptions.Single());
                }
            }
            else
            {
                throw new GeneralExceptionDuringAzureOperationException("Azure service request received an exception without a valid Http Response to parse", exception);
            }
        }

        private Exception GetAzureExceptionFor(Response<ErrorResponsePayload> response, WebException originalException)
        {
            switch (ServiceType)
            {
                case StorageServiceType.QueueService:
                    return QueueServiceAzureExceptions.GetExceptionFor(response.RequestId, response.HttpStatus, response.Payload.ErrorCode, response.Payload.ErrorMessage, originalException);
                case StorageServiceType.BlobService:
                    return BlobServiceAzureExceptions.GetExceptionFor(response.RequestId, response.HttpStatus, response.Payload.ErrorCode, response.Payload.ErrorMessage, originalException);
                case StorageServiceType.TableService:
                    return TableServiceAzureExceptions.GetExceptionFor(response.RequestId, response.HttpStatus, response.Payload.ErrorCode, response.Payload.ErrorMessage, originalException);
                default:
                    return CommonServiceAzureExceptions.GetExceptionFor(response.RequestId, response.HttpStatus, response.Payload.ErrorCode, response.Payload.ErrorMessage, originalException);
            }
        }

    }
}
