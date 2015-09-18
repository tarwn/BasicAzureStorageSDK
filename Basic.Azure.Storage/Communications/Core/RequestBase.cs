﻿using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Microsoft.Practices.TransientFaultHandling;
using System;
using System.Collections.Concurrent;
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

        private readonly StorageAccountSettings _settings;

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

        private bool HasContentToSend
        {
            get
            {
                return typeof(ISendDataWithRequest).IsAssignableFrom(this.GetType());
            }
        }

        private bool HasAdditionalRequiredHeaders
        {
            get
            {
                return typeof(ISendAdditionalRequiredHeaders).IsAssignableFrom(this.GetType());
            }
        }

        private bool HasAdditionalOptionalHeaders
        {
            get
            {
                return typeof(ISendAdditionalOptionalHeaders).IsAssignableFrom(this.GetType());
            }
        }

        public Response<TPayload> Execute(ConcurrentDictionary<string,string> responseCodeOverridesForApiBugs = null)
        {
            try
            {
                return Task.Run(() => ExecuteAsync(responseCodeOverridesForApiBugs)).Result;
            }
            catch (AggregateException ae)
            {
                throw ae.Flatten().InnerExceptions.First();
            }
        }

        public async Task<Response<TPayload>> ExecuteAsync(ConcurrentDictionary<string, string> responseCodeOverridesForApiBugs = null)
        {
            // send web request
            return await SendRequestWithRetryAsync(responseCodeOverridesForApiBugs);
        }

        public WebRequest BuildRequest()
        {
            // create web request
            var requestUri = GetUriBase();
            var request = WebRequest.Create(requestUri.GetUri());

            request.Method = HttpMethod;
            if (HasContentToSend)
                request.ContentLength = ((ISendDataWithRequest)this).GetContentLength();
            else
                request.ContentLength = 0;

            {
                // hacky workaround area
                //  - Can't test against HttpWebRequest because the ctor isn't accessible
                //  - Can't set UserAgent as raw header because HttpWebRequest won't allow you to not use the named property
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

            return request;
        }

        private void ApplyRequiredHeaders(WebRequest request)
        {
            if (HasAdditionalRequiredHeaders)
                ((ISendAdditionalRequiredHeaders)this).ApplyAdditionalRequiredHeaders(request);
        }

        private void ApplyOptionalHeaders(WebRequest request)
        {
            if (HasAdditionalOptionalHeaders)
                ((ISendAdditionalOptionalHeaders)this).ApplyAdditionalOptionalHeaders(request);
        }

        private static void ApplyAuthorizationHeader(StorageServiceType serviceType, WebRequest request, Dictionary<string, string> queryStringParameters, StorageAccountSettings settings)
        {
            switch (serviceType)
            {
                case StorageServiceType.QueueService:
                    var queueSignedString = SignedAuthorization.GenerateSharedKeySignatureString(request, queryStringParameters, settings);
                    request.Headers.Add(ProtocolConstants.Headers.Authorization, String.Format("SharedKey {0}:{1}", settings.AccountName, queueSignedString));
                    break;
                case StorageServiceType.BlobService:
                    var blobSignedString = SignedAuthorization.GenerateSharedKeySignatureString(request, queryStringParameters, settings);
                    request.Headers.Add(ProtocolConstants.Headers.Authorization, String.Format("SharedKey {0}:{1}", settings.AccountName, blobSignedString));
                    break;
                case StorageServiceType.TableService:
                    var tableSignedString = SignedAuthorization.GenerateSharedKeySignatureStringForTableService(request, queryStringParameters, settings);
                    request.Headers.Add(ProtocolConstants.Headers.Authorization, String.Format("SharedKey {0}:{1}", settings.AccountName, tableSignedString));
                    //string tableSignedString = SignedAuthorization.GenerateSharedKeyLiteSignatureStringForTableService(request, queryStringParameters, settings);
                    //request.Headers.Add(ProtocolConstants.Headers.Authorization, String.Format("SharedKeyLite {0}:{1}", settings.AccountName, tableSignedString));
                    break;
                default:
                    throw new NotImplementedException("Specified authentication type is not supported yet");
            }
        }

        private async Task<Response<TPayload>> SendRequestWithRetryAsync(ConcurrentDictionary<string, string> responseCodeOverridesForApiBugs = null)
        {
            var numberOfAttempts = 0;
            try
            {
                return await RetryPolicy.ExecuteAsync(async () =>
                {
                    numberOfAttempts++;
                    try
                    {
                        var result = await SendRequestAsync();
                        result.NumberOfAttempts = numberOfAttempts;
                        return result;
                    }
                    catch (Exception exc)
                    {
                        throw GetAzureExceptionForAsync(exc, responseCodeOverridesForApiBugs).Result;
                    }
                });
            }
            catch (Exception exc)
            {
                if (numberOfAttempts > 1)
                    throw new RetriedException(exc, numberOfAttempts);
                else
                    throw;
            }
        }

        private async Task<Response<TPayload>> SendRequestAsync()
        {
            var request = BuildRequest();
            if (HasContentToSend)
            {
                var stream = await request.GetRequestStreamAsync();
                var content = ((ISendDataWithRequest)this).GetContentToSend();
                await stream.WriteAsync(content, 0, content.Length);
            }
            var response = await request.GetResponseAsync();
            return await ReceiveResponseAsync((HttpWebResponse)response);
        }

        private static async Task<Response<TPayload>> ReceiveResponseAsync(HttpWebResponse httpWebResponse)
        {
            var response = new Response<TPayload>(httpWebResponse);
            await response.ProcessResponseStreamAsync(httpWebResponse);
            return response;
        }

        private async Task<Exception> GetAzureExceptionForAsync(Exception exception, ConcurrentDictionary<string, string> responseCodeOverridesForApiBugs = null)
        {
            if (exception is WebException)
            {
                var wexc = (WebException)exception;
                if (wexc.Response != null && typeof(HttpWebResponse).IsAssignableFrom(wexc.Response.GetType()))
                {
                    var response = new Response<ErrorResponsePayload>((HttpWebResponse)wexc.Response);
                    await response.ProcessResponseStreamAsync((HttpWebResponse)wexc.Response);
                    return GetAzureExceptionFor(response, wexc, responseCodeOverridesForApiBugs);
                }
                else
                {
                    return new UnidentifiedAzureException(wexc);
                }
            }
            else
            {
                throw new GeneralExceptionDuringAzureOperationException("Azure service request received an exception without a valid Http Response to parse", exception);
            }
        }

        private Exception GetAzureExceptionFor(Response<ErrorResponsePayload> response, WebException originalException, ConcurrentDictionary<string, string> responseCodeOverridesForApiBugs = null)
        {
            string errorCode = response.Payload.ErrorCode;
            if (responseCodeOverridesForApiBugs != null && responseCodeOverridesForApiBugs.ContainsKey(errorCode))
            {
                string originalErrorCode = errorCode;
                responseCodeOverridesForApiBugs.TryGetValue(originalErrorCode, out errorCode);
            }

            switch (ServiceType)
            {
                case StorageServiceType.QueueService:
                    return QueueServiceAzureExceptions.GetExceptionFor(response.RequestId, response.HttpStatus, errorCode, response.Payload.ErrorMessage, response.Payload.Details, originalException);
                case StorageServiceType.BlobService:
                    return BlobServiceAzureExceptions.GetExceptionFor(response.RequestId, response.HttpStatus, errorCode, response.Payload.ErrorMessage, response.Payload.Details, originalException);
                case StorageServiceType.TableService:
                    return TableServiceAzureExceptions.GetExceptionFor(response.RequestId, response.HttpStatus, errorCode, response.Payload.ErrorMessage, response.Payload.Details, originalException);
                default:
                    return CommonServiceAzureExceptions.GetExceptionFor(response.RequestId, response.HttpStatus, errorCode, response.Payload.ErrorMessage, response.Payload.Details, originalException);
            }
        }

    }
}
