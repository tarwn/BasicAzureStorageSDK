using Basic.Azure.Storage.Communications.Core;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Basic.Azure.Storage.Tests.Fakes;
using Microsoft.Practices.TransientFaultHandling;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using TestableHttpWebResponse;
using TestableHttpWebResponse.ResponseSettings;

namespace Basic.Azure.Storage.Tests.Communications.Core
{
    [TestFixture]
    public class RequestBaseTests_AzureExceptions
    {
        // These tests are a little overkill, we're basically testing generated logic.
        // Update the generated entries first and let the tests runs, that will let us
        //  know if any older (valid) API Error codes have been removed from the documentation.
        //  We're going to want to continue supporting them.

        [TestFixtureSetUp]
        public void SetupFixture()
        {
            WebRequest.RegisterPrefix("test", TestableWebRequestCreateFactory.GetFactory());
        }

        // "Common REST API Error Codes" - http://msdn.microsoft.com/en-us/library/windowsazure/dd179438.aspx
        //  Copy the content of the table, use this regex to generate test cases:
        //      Search:  ([A-Za-z0-9]+)[\r\n]+[A-Za-z ]+\((\d+)\)[\r\n]+([A-Za-z0-9 \d\-\.\,\/\(\)\']+)[\r\n]+
        //      Replace: [TestCase\("\1","\1",\2,"\3"\)]\n
        // FirstArg is what we expect the exception to be named so we can handle duplicates:
        //  ConditionNotMet => Append ForRead, ForWrite 
        //  InsufficientAccountPermissions => Append ForRead, ForWrite , ForExecute
        [TestCase("ConditionNotMetForRead", "ConditionNotMet", 304, "The condition specified in the conditional header(s) was not met for a read operation.")]
        [TestCase("MissingRequiredHeader", "MissingRequiredHeader", 400, "A required HTTP header was not specified.")]
        [TestCase("MissingRequiredXmlNode", "MissingRequiredXmlNode", 400, "A required XML node was not specified in the request body.")]
        [TestCase("UnsupportedHeader", "UnsupportedHeader", 400, "One of the HTTP headers specified in the request is not supported.")]
        [TestCase("UnsupportedXmlNode", "UnsupportedXmlNode", 400, "One of the XML nodes specified in the request body is not supported.")]
        [TestCase("InvalidHeaderValue", "InvalidHeaderValue", 400, "The value provided for one of the HTTP headers was not in the correct format.")]
        [TestCase("InvalidXmlNodeValue", "InvalidXmlNodeValue", 400, "The value provided for one of the XML nodes in the request body was not in the correct format.")]
        [TestCase("MissingRequiredQueryParameter", "MissingRequiredQueryParameter", 400, "A required query parameter was not specified for this request.")]
        [TestCase("UnsupportedQueryParameter", "UnsupportedQueryParameter", 400, "One of the query parameters specified in the request URI is not supported.")]
        [TestCase("InvalidQueryParameterValue", "InvalidQueryParameterValue", 400, "An invalid value was specified for one of the query parameters in the request URI.")]
        [TestCase("OutOfRangeQueryParameterValue", "OutOfRangeQueryParameterValue", 400, "A query parameter specified in the request URI is outside the permissible range.")]
        [TestCase("RequestUrlFailedToParse", "RequestUrlFailedToParse", 400, "The url in the request could not be parsed.")]
        [TestCase("InvalidUri", "InvalidUri", 400, "The requested URI does not represent any resource on the server.")]
        [TestCase("InvalidHttpVerb", "InvalidHttpVerb", 400, "The HTTP verb specified was not recognized by the server.")]
        [TestCase("EmptyMetadataKey", "EmptyMetadataKey", 400, "The key for one of the metadata key-value pairs is empty.")]
        [TestCase("InvalidXmlDocument", "InvalidXmlDocument", 400, "The specified XML is not syntactically valid.")]
        [TestCase("Md5Mismatch", "Md5Mismatch", 400, "The MD5 value specified in the request did not match the MD5 value calculated by the server.")]
        [TestCase("InvalidMd5", "InvalidMd5", 400, "The MD5 value specified in the request is invalid. The MD5 value must be 128 bits and Base64-encoded.")]
        [TestCase("OutOfRangeInput", "OutOfRangeInput", 400, "One of the request inputs is out of range.")]
        [TestCase("InvalidAuthenticationInfo", "InvalidAuthenticationInfo", 400, "The authentication information was not provided in the correct format. Verify the value of Authorization header.")]
        [TestCase("InvalidInput", "InvalidInput", 400, "One of the request inputs is not valid.")]
        [TestCase("InvalidMetadata", "InvalidMetadata", 400, "The specified metadata is invalid. It includes characters that are not permitted.")]
        [TestCase("MetadataTooLarge", "MetadataTooLarge", 400, "The size of the specified metadata exceeds the maximum size permitted.")]
        [TestCase("AuthenticationFailed", "AuthenticationFailed", 403, "Server failed to authenticate the request. Make sure the value of the Authorization header is formed correctly including the signature.")]
        [TestCase("InsufficientAccountPermissionsForRead", "InsufficientAccountPermissions", 403, "Read-access geo-redundant replication is not enabled for the account.")]
        [TestCase("InsufficientAccountPermissionsForWrite", "InsufficientAccountPermissions", 403, "Write operations to the secondary location are not allowed.")]
        [TestCase("ResourceNotFound", "ResourceNotFound", 404, "The specified resource does not exist.")]
        [TestCase("AccountIsDisabled", "AccountIsDisabled", 403, "The specified account is disabled.")]
        [TestCase("UnsupportedHttpVerb", "UnsupportedHttpVerb", 405, "The resource doesn't support the specified HTTP verb.")]
        [TestCase("AccountAlreadyExists", "AccountAlreadyExists", 409, "The specified account already exists.")]
        [TestCase("AccountBeingCreated", "AccountBeingCreated", 409, "The specified account is in the process of being created.")]
        [TestCase("InsufficientAccountPermissionsForExecute", "InsufficientAccountPermissions", 403, "The account being accessed does not have sufficient permissions to execute this operation.")]
        [TestCase("MissingContentLengthHeader", "MissingContentLengthHeader", 411, "The Content-Length header was not specified.")]
        [TestCase("ConditionNotMetForWrite", "ConditionNotMet", 412, "The condition specified in the conditional header(s) was not met for a write operation.")]
        [TestCase("MultipleConditionHeadersNotSupported", "MultipleConditionHeadersNotSupported", 400, "Multiple condition headers are not supported.")]
        [TestCase("RequestBodyTooLarge", "RequestBodyTooLarge", 413, "The size of the request body exceeds the maximum size permitted.")]
        [TestCase("InvalidRange", "InvalidRange", 416, "The range specified is invalid for the current size of the resource.")]
        [TestCase("InternalError", "InternalError", 500, "The server encountered an internal error. Please retry the request.")]
        [TestCase("OperationTimedOut", "OperationTimedOut", 500, "The operation could not be completed within the permitted time.")]
        [TestCase("ServerBusy", "ServerBusy", 503, "The server is currently unable to receive requests. Please retry your request.")]
        public void Execute_CommonApiException_MapsToProperAzureException(string exceptionName, string errorCode, int httpStatus, string errorMessage)
        {
            //arrange
            string expectedExceptionName = exceptionName + "AzureException";
            var expectedUri = "test://common-api-errors.abc/whatever/";
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri));
            expectedRawRequest.EnqueueResponse((HttpStatusCode)httpStatus, errorCode, ErrorContentFor(errorCode, errorMessage), true);
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var request = new RequestWithErrorPayload(new SettingsFake(), expectedUri, "GET", StorageServiceType.QueueService);
            request.RetryPolicy = new RetryPolicy<ExceptionRetryStrategy>(0);
            try
            {
                //act
                var response = request.Execute();
            }
            catch (AzureException ae)
            {
                //assert
                Assert.AreEqual(expectedExceptionName, ae.GetType().Name);
            }
        }

        // "Queue REST API Error Codes" - http://msdn.microsoft.com/en-us/library/windowsazure/dd179446.aspx
        //  Copy the content of the table, use this regex to generate test cases:
        //      Search:  ([A-Za-z0-9]+)[\r\n]+[A-Za-z ]+\((\d+)\)[\r\n]+([A-Za-z0-9 \d\-\.\,\/\(\)\']+)[\r\n]+
        //      Replace: [TestCase\("\1","\1",\2,"\3"\)]\n
        [TestCase("MessageTooLarge", "MessageTooLarge", 400, "The message exceeds the maximum allowed size.")]
        [TestCase("InvalidMarker", "InvalidMarker", 400, "The specified marker is invalid.")]
        [TestCase("PopReceiptMismatch", "PopReceiptMismatch", 400, "The specified pop receipt did not match the pop receipt for a dequeued message.")]
        [TestCase("QueueNotFound", "QueueNotFound", 404, "The specified queue does not exist.")]
        [TestCase("MessageNotFound", "MessageNotFound", 404, "The specified message does not exist.")]
        [TestCase("QueueDisabled", "QueueDisabled", 409, "The specified queue has been disabled by the administrator.")]
        [TestCase("QueueAlreadyExists", "QueueAlreadyExists", 409, "The specified queue already exists.")]
        [TestCase("QueueBeingDeleted", "QueueBeingDeleted", 409, "The specified queue is being deleted.")]
        [TestCase("QueueNotEmpty", "QueueNotEmpty", 409, "The specified queue is not empty.")]
        public void Execute_QueueApiException_MapsToProperAzureException(string exceptionName, string errorCode, int httpStatus, string errorMessage)
        {
            //arrange
            string expectedExceptionName = exceptionName + "AzureException";
            var expectedUri = "test://common-api-errors.abc/whatever/";
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri));
            expectedRawRequest.EnqueueResponse((HttpStatusCode)httpStatus, errorCode, ErrorContentFor(errorCode, errorMessage), true);
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var request = new RequestWithErrorPayload(new SettingsFake(), expectedUri, "GET", StorageServiceType.QueueService);
            request.RetryPolicy = new RetryPolicy<ExceptionRetryStrategy>(0);
            try
            {
                //act
                var response = request.Execute();
            }
            catch (AzureException ae)
            {
                //assert
                Assert.AreEqual(expectedExceptionName, ae.GetType().Name);
            }
        }

        // "Blob REST API Error Codes" - http://msdn.microsoft.com/en-us/library/windowsazure/dd179439.aspx
        //  Copy the content of the table, use this regex to generate test cases:
        //      Search:  ([A-Za-z0-9]+)[\r\n]+[A-Za-z ]+\((\d+)\)[\r\n]+([A-Za-z0-9 \d\-\.\,\/\(\)\']+)[\r\n]+
        //      Replace: [TestCase\("\1","\1",\2,"\3"\)]\n
        [TestCase("InvalidBlobOrBlock", "InvalidBlobOrBlock", 400, "The specified blob or block content is invalid.")]
        [TestCase("InvalidBlockId", "InvalidBlockId", 400, "The specified block ID is invalid. The block ID must be Base64-encoded.")]
        [TestCase("InvalidBlockList", "InvalidBlockList", 400, "The specified block list is invalid.")]
        [TestCase("ContainerNotFound", "ContainerNotFound", 404, "The specified container does not exist.")]
        [TestCase("BlobNotFound", "BlobNotFound", 404, "The specified blob does not exist.")]
        [TestCase("ContainerAlreadyExists", "ContainerAlreadyExists", 409, "The specified container already exists.")]
        [TestCase("ContainerDisabled", "ContainerDisabled", 409, "The specified container has been disabled by the administrator.")]
        [TestCase("ContainerBeingDeleted", "ContainerBeingDeleted", 409, "The specified container is being deleted.")]
        [TestCase("BlobAlreadyExists", "BlobAlreadyExists", 409, "The specified blob already exists.")]
        [TestCase("LeaseNotPresentWithBlobOperation", "LeaseNotPresentWithBlobOperation", 412, "There is currently no lease on the blob.")]
        [TestCase("LeaseNotPresentWithContainerOperation", "LeaseNotPresentWithContainerOperation", 412, "There is currently no lease on the container.")]
        [TestCase("LeaseLost", "LeaseLost", 412, "A lease ID was specified, but the lease for the blob/container has expired.")]
        [TestCase("LeaseIdMismatchWithBlobOperation", "LeaseIdMismatchWithBlobOperation", 412, "The lease ID specified did not match the lease ID for the blob.")]
        [TestCase("LeaseIdMismatchWithContainerOperation", "LeaseIdMismatchWithContainerOperation", 412, "The lease ID specified did not match the lease ID for the container.")]
        [TestCase("LeaseIdMissing", "LeaseIdMissing", 412, "There is currently a lease on the blob/container and no lease ID was specified in the request.")]
        [TestCase("LeaseNotPresentWithLeaseOperation", "LeaseNotPresentWithLeaseOperation", 409, "There is currently no lease on the blob/container.")]
        [TestCase("LeaseIdMismatchWithLeaseOperation", "LeaseIdMismatchWithLeaseOperation", 409, "The lease ID specified did not match the lease ID for the blob/container.")]
        [TestCase("LeaseAlreadyPresent", "LeaseAlreadyPresent", 409, "There is already a lease present.")]
        [TestCase("LeaseAlreadyBroken", "LeaseAlreadyBroken", 409, "The lease has already been broken and cannot be broken again.")]
        [TestCase("LeaseIsBrokenAndCannotBeRenewed", "LeaseIsBrokenAndCannotBeRenewed", 409, "The lease ID matched, but the lease has been broken explicitly and cannot be renewed.")]
        [TestCase("LeaseIsBreakingAndCannotBeAquired", "LeaseIsBreakingAndCannotBeAquired", 409, "The lease ID matched, but the lease is currently in breaking state and cannot be acquired until it is broken.")]
        [TestCase("LeaseIsBreakingAndCannotBeChanged", "LeaseIsBreakingAndCannotBeChanged", 409, "The lease ID matched, but the lease is currently in breaking state and cannot be changed.")]
        [TestCase("InfiniteLeaseDurationRequired", "InfiniteLeaseDurationRequired", 412, "The lease ID matched, but the specified lease must be an infinite-duration lease.")]
        [TestCase("SnapshotsPresent", "SnapshotsPresent", 409, "This operation is not permitted because the blob has snapshots.")]
        [TestCase("InvalidBlobType", "InvalidBlobType", 409, "The blob type is invalid for this operation.")]
        [TestCase("InvalidVersionForPageBlobOperation", "InvalidVersionForPageBlobOperation", 400, "All operations on page blobs require at least version 2009-09-19.")]
        [TestCase("InvalidPageRange", "InvalidPageRange", 416, "The page range specified is invalid.")]
        [TestCase("SequenceNumberConditionNotMet", "SequenceNumberConditionNotMet", 412, "The sequence number condition specified was not met.")]
        [TestCase("SequenceNumberIncrementTooLarge", "SequenceNumberIncrementTooLarge", 409, "The sequence number increment cannot be performed because it would result in overflow of the sequence number.")]
        [TestCase("SourceConditionNotMet", "SourceConditionNotMet", 412, "The source condition specified using HTTP conditional header(s) is not met.")]
        [TestCase("TargetConditionNotMet", "TargetConditionNotMet", 412, "The target condition specified using HTTP conditional header(s) is not met.")]
        [TestCase("CopyAcrossAccountsNotSupported", "CopyAcrossAccountsNotSupported", 400, "The copy source account and destination account must be the same.")]
        [TestCase("CannotVerifyCopySource", "CannotVerifyCopySource", 500, "Could not verify the copy source within the specified time. Examine the HTTP status code and message for more information about the failure.")]
        [TestCase("PendingCopyOperation", "PendingCopyOperation", 409, "There is currently a pending copy operation.")]
        [TestCase("NoPendingCopyOperation", "NoPendingCopyOperation", 409, "There is currently no pending copy operation.")]
        [TestCase("CopyIdMismatch", "CopyIdMismatch", 409, "The specified copy ID did not match the copy ID for the pending copy operation.")]
        public void Execute_BlobApiException_MapsToProperAzureException(string exceptionName, string errorCode, int httpStatus, string errorMessage)
        {
            //arrange
            string expectedExceptionName = exceptionName + "AzureException";
            var expectedUri = "test://common-api-errors.abc/whatever/";
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri));
            expectedRawRequest.EnqueueResponse((HttpStatusCode)httpStatus, errorCode, ErrorContentFor(errorCode, errorMessage), true);
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var request = new RequestWithErrorPayload(new SettingsFake(), expectedUri, "GET", StorageServiceType.BlobService);
            request.RetryPolicy = new RetryPolicy<ExceptionRetryStrategy>(0);
            try
            {
                //act
                var response = request.Execute();
            }
            catch (AzureException ae)
            {
                //assert
                Assert.AreEqual(expectedExceptionName, ae.GetType().Name);
            }
        }

        /*
         * Can't uncomment this until I implement authorization header logic for table service

        // "Table REST API Error Codes" - http://msdn.microsoft.com/en-us/library/windowsazure/dd179438.aspx
        //  Copy the content of the table, use this regex to generate test cases:
        //      Search:  ([A-Za-z0-9]+)[\r\n]+[A-Za-z ]+\((\d+)\)[\r\n]+([A-Za-z0-9 \d\-\.\,\/\(\)\']+)[\r\n]+
        //      Replace: [TestCase\("\1","\1",\2,"\3"\)]\n
        [TestCase("DuplicatePropertiesSpecified", "DuplicatePropertiesSpecified", 400, "A property is specified more than one time.")]
        [TestCase("EntityAlreadyExists", "EntityAlreadyExists", 409, "The specified entity already exists.")]
        [TestCase("EntityTooLarge", "EntityTooLarge", 400, "The entity is larger than the maximum size permitted.")]
        [TestCase("HostInformationNotPresent", "HostInformationNotPresent", 400, "The required host information is not present in the request. You must send a non-empty Host header or include the absolute URI in the request line.")]
        [TestCase("InvalidValueType", "InvalidValueType", 400, "The value specified is invalid.")]
        [TestCase("JsonFormatNotSupported", "JsonFormatNotSupported", 415, "JSON format is not supported.")]
        [TestCase("MethodNotAllowed", "MethodNotAllowed", 405, "The requested method is not allowed on the specified resource.")]
        [TestCase("NotImplemented", "NotImplemented", 501, "The requested operation is not implemented on the specified resource.")]
        [TestCase("PropertiesNeedValue", "PropertiesNeedValue", 400, "Values have not been specified for all properties in the entity.")]
        [TestCase("PropertyNameInvalid", "PropertyNameInvalid", 400, "The property name is invalid.")]
        [TestCase("PropertyNameTooLong", "PropertyNameTooLong", 400, "The property name exceeds the maximum allowed length.")]
        [TestCase("PropertyValueTooLarge", "PropertyValueTooLarge", 400, "The property value is larger than the maximum size permitted.")]
        [TestCase("TableAlreadyExists", "TableAlreadyExists", 409, "The table specified already exists.")]
        [TestCase("TableBeingDeleted", "TableBeingDeleted", 409, "The specified table is being deleted.")]
        [TestCase("TableNotFound", "TableNotFound", 404, "The table specified does not exist.")]
        [TestCase("TooManyProperties", "TooManyProperties", 400, "The entity contains more properties than allowed.")]
        [TestCase("UpdateConditionNotSatisfied", "UpdateConditionNotSatisfied", 412, "The update condition specified in the request was not satisfied.")]
        [TestCase("XMethodIncorrectCount", "XMethodIncorrectCount", 400, "More than one X-HTTP-Method is specified.")]
        [TestCase("XMethodIncorrectValue", "XMethodIncorrectValue", 400, "The specified X-HTTP-Method is invalid.")]
        [TestCase("XMethodNotUsingPost", "XMethodNotUsingPost", 400, "The request uses X-HTTP-Method with an HTTP verb other than POST.")]
        public void Execute_TableApiException_MapsToProperAzureException(string exceptionName, string errorCode, int httpStatus, string errorMessage)
        {
            //arrange
            string expectedExceptionName = exceptionName + "AzureException";
            var expectedUri = "test://common-api-errors.abc/whatever/";
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri));
            expectedRawRequest.EnqueueResponse((HttpStatusCode)httpStatus, errorCode, ErrorContentFor(errorCode, errorMessage), true);
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var request = new RequestWithErrorPayload(new SettingsFake(), expectedUri, "GET", StorageServiceType.TableService);
            request.RetryPolicy = new RetryPolicy<ExceptionRetryStrategy>(0);
            try
            {
                //act
                var response = request.Execute();
            }
            catch (AzureException ae)
            {
                //assert
                Assert.AreEqual(expectedExceptionName, ae.GetType().Name);
            }
        }

        */


        private string ErrorContentFor(string errorCode, string errorMessage)
        {
            return String.Format("<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>{0}</Code><Message>{1}</Message></Error>", errorCode, errorMessage);
        }

    }
}
