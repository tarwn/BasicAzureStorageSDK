using Basic.Azure.Storage.Communications.Core.Interfaces;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Microsoft.Practices.TransientFaultHandling;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Core
{
    public abstract class RequestBase<TPayload>
        where TPayload : IResponsePayload, new()
    {
        // Versioning for Azure Storage Services doc:
        //      https://docs.microsoft.com/en-us/rest/api/storageservices/versioning-for-the-azure-storage-services

        public const string TargetStorageVersion = "2015-07-08";

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
                return this is ISendDataWithRequest;
            }
        }

        private bool HasAdditionalRequiredHeaders
        {
            get
            {
                return this is ISendAdditionalRequiredHeaders;
            }
        }

        private bool HasAdditionalOptionalHeaders
        {
            get
            {
                return this is ISendAdditionalOptionalHeaders;
            }
        }

        public Response<TPayload> Execute(RetryPolicy optionalRetryPolicy = null, ConcurrentDictionary<string, string> responseCodeOverridesForApiBugs = null)
        {
            try
            {
                return Task.Run(() => ExecuteAsync(optionalRetryPolicy, responseCodeOverridesForApiBugs)).Result;
            }
            catch (AggregateException ae)
            {
                throw ae.Flatten().InnerExceptions.First();
            }
        }

        public async Task<Response<TPayload>> ExecuteAsync(RetryPolicy optionalRetryPolicy = null, ConcurrentDictionary<string, string> responseCodeOverridesForApiBugs = null)
        {
            // send web request
            return await SendRequestWithRetryAsync(optionalRetryPolicy, responseCodeOverridesForApiBugs);
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
                    ((HttpWebRequest)request).UserAgent = "Basic.Azure.Storage/1.1.0";
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

        private async Task<Response<TPayload>> SendRequestWithRetryAsync(RetryPolicy optionalRetryPolicy = null, ConcurrentDictionary<string, string> responseCodeOverridesForApiBugs = null)
        {
            var numberOfAttempts = 0;
            var retryStack = new Stack<Exception>();
            var retryPolicy = optionalRetryPolicy ?? RetryPolicy;
            try
            {
                return await retryPolicy.ExecuteAsync(async () =>
                {
                    try
                    {
                        numberOfAttempts++;
                        return await SendSingleRequest(numberOfAttempts, responseCodeOverridesForApiBugs);
                    }
                    catch (Exception ex)
                    {
                        retryStack.Push(ex);
                        throw;
                    }
                });
            }
            catch (Exception exc)
            {
                if (retryPolicy.ErrorDetectionStrategy.IsTransient(exc) && numberOfAttempts > 1)
                {
                    throw new RetriedException(exc, numberOfAttempts, retryStack);
                }
                else
                {
                    var azureException = exc as AzureException;
                    if (null != azureException)
                    {
                        azureException.ExceptionRetryStack = retryStack;
                        azureException.RetryCount = retryStack.Count;
                    }

                    throw exc;
                }
            }
        }

        private async Task<Response<TPayload>> SendSingleRequest(int attemptNumber, ConcurrentDictionary<string, string> responseCodeOverridesForApiBugs = null)
        {
            Response<TPayload> result = null;
            Exception exception = null;

            try
            {
                result = await SendRequestAsync();
                result.NumberOfAttempts = attemptNumber;
            }
            catch (Exception exc)
            {
                // this wonkiness is due to not being able to handle async code in a catch
                // we can change this in C# 6
                exception = exc;
            }
            if (null != exception)
            {
                throw await GetAzureExceptionForAsync(exception, responseCodeOverridesForApiBugs);
            }

            return result;
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
            var webException = exception as WebException;
            if (null != webException)
            {
                var httpWebResponse = webException.Response as HttpWebResponse;
                if (null != httpWebResponse)
                {
                    var response = new Response<ErrorResponsePayload>(httpWebResponse);
                    await response.ProcessResponseStreamAsync(httpWebResponse);
                    return GetAzureExceptionFor(response, webException, responseCodeOverridesForApiBugs);
                }
                else
                {
                    return new UnidentifiedAzureException(webException);
                }
            }
            else
            {
                throw new GeneralExceptionDuringAzureOperationException("Azure service request received an exception without a valid Http Response to parse", exception);
            }
        }

        private Exception GetAzureExceptionFor(Response<ErrorResponsePayload> response, WebException originalException, ConcurrentDictionary<string, string> responseCodeOverridesForApiBugs = null)
        {
            var errorCode = response.Payload.ErrorCode;
            if (responseCodeOverridesForApiBugs != null && responseCodeOverridesForApiBugs.ContainsKey(errorCode))
            {
                var originalErrorCode = errorCode;
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
