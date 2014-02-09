using Basic.Azure.Storage.Communications.ServiceExceptions;
using Microsoft.Practices.TransientFaultHandling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Core
{
    public class ExceptionRetryStrategy : ITransientErrorDetectionStrategy
    {
        public bool IsTransient(Exception ex)
        {
            if (ex.InnerException != null && ex.InnerException is WebException)
            {
                var webException = ex.InnerException as WebException;
                if (webException != null && (
                    // re-used conection that wasn't kept alive
                    webException.Status == WebExceptionStatus.ConnectionClosed
                    // underlying connection closed while receiving - Azure hung up on us
                    || webException.Status == WebExceptionStatus.ConnectionClosed
                    // underlying connection closed while receiving - Azure hung up on us
                    || webException.Status == WebExceptionStatus.ReceiveFailure
                    // azure refused the connections - this may be the DDOS protection logic?
                    || webException.Status == WebExceptionStatus.ConnectFailure
                    // Keep Alive failed when getting response?
                    || webException.Status == WebExceptionStatus.KeepAliveFailure))
                {
                    return true;
                }
            }

            var exceptionToReview = ex;
            if (ex.GetType().IsAssignableFrom(typeof(GeneralExceptionDuringAzureOperationException)) && ex.InnerException != null)
                exceptionToReview = ex.InnerException;

            return exceptionToReview is InternalErrorAzureException
                || exceptionToReview is ServerBusyAzureException
                || exceptionToReview is OperationTimedOutAzureException
                /* || ex is "TableErrorCodeStrings.TableServerOutOfMemory" */
                || exceptionToReview is TimeoutException
                || exceptionToReview is UnidentifiedAzureException;

        }
    }
}
