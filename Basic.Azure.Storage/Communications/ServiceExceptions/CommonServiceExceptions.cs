﻿// -----------------------------------------------------------------------------
// Autogenerated code. Do not modify.
// -----------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;

namespace Basic.Azure.Storage.Communications.ServiceExceptions
{

/// <summary>
/// Maps a WebException from Azure to the appropriate local exception type
/// </summary>
/// <remarks>
/// Uses the Common Service Error Codes defined here: http://msdn.microsoft.com/en-us/library/windowsazure/dd179357.aspx
/// </remarks>
public static class CommonServiceAzureExceptions
{

	public static AzureException GetExceptionFor(string requestId, HttpStatusCode statusCode, string errorCode, string statusDescription, WebException baseException)
	{
		switch(errorCode)
		{
		 
			case "ConditionNotMetForRead":
				return new ConditionNotMetForReadAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "MissingRequiredHeader":
				return new MissingRequiredHeaderAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "MissingRequiredXmlNode":
				return new MissingRequiredXmlNodeAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "UnsupportedHeader":
				return new UnsupportedHeaderAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "UnsupportedXmlNode":
				return new UnsupportedXmlNodeAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "InvalidHeaderValue":
				return new InvalidHeaderValueAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "InvalidXmlNodeValue":
				return new InvalidXmlNodeValueAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "MissingRequiredQueryParameter":
				return new MissingRequiredQueryParameterAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "UnsupportedQueryParameter":
				return new UnsupportedQueryParameterAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "InvalidQueryParameterValue":
				return new InvalidQueryParameterValueAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "OutOfRangeQueryParameterValue":
				return new OutOfRangeQueryParameterValueAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "RequestUrlFailedToParse":
				return new RequestUrlFailedToParseAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "InvalidUri":
				return new InvalidUriAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "InvalidHttpVerb":
				return new InvalidHttpVerbAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "EmptyMetadataKey":
				return new EmptyMetadataKeyAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "InvalidXmlDocument":
				return new InvalidXmlDocumentAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "Md5Mismatch":
				return new Md5MismatchAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "InvalidMd5":
				return new InvalidMd5AzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "OutOfRangeInput":
				return new OutOfRangeInputAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "InvalidAuthenticationInfo":
				return new InvalidAuthenticationInfoAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "InvalidInput":
				return new InvalidInputAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "InvalidMetadata":
				return new InvalidMetadataAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "MetadataTooLarge":
				return new MetadataTooLargeAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "AuthenticationFailed":
				return new AuthenticationFailedAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "InsufficientAccountPermissionsForRead":
				return new InsufficientAccountPermissionsForReadAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "InsufficientAccountPermissionsForWrite":
				return new InsufficientAccountPermissionsForWriteAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "ResourceNotFound":
				return new ResourceNotFoundAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "AccountIsDisabled":
				return new AccountIsDisabledAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "UnsupportedHttpVerb":
				return new UnsupportedHttpVerbAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "AccountAlreadyExists":
				return new AccountAlreadyExistsAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "AccountBeingCreated":
				return new AccountBeingCreatedAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "InsufficientAccountPermissionsForExecute":
				return new InsufficientAccountPermissionsForExecuteAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "MissingContentLengthHeader":
				return new MissingContentLengthHeaderAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "ConditionNotMetForWrite":
				return new ConditionNotMetForWriteAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "MultipleConditionHeadersNotSupported":
				return new MultipleConditionHeadersNotSupportedAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "RequestBodyTooLarge":
				return new RequestBodyTooLargeAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "InvalidRange":
				return new InvalidRangeAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "InternalError":
				return new InternalErrorAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "OperationTimedOut":
				return new OperationTimedOutAzureException(requestId, statusCode, statusDescription, baseException);
			 
			case "ServerBusy":
				return new ServerBusyAzureException(requestId, statusCode, statusDescription, baseException);
					}

		switch(statusDescription)
		{
			 
				case "The condition specified in the conditional header(s) was not met for a read operation.":
					return new ConditionNotMetForReadAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "A required HTTP header was not specified.":
					return new MissingRequiredHeaderAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "A required XML node was not specified in the request body.":
					return new MissingRequiredXmlNodeAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "One of the HTTP headers specified in the request is not supported.":
					return new UnsupportedHeaderAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "One of the XML nodes specified in the request body is not supported.":
					return new UnsupportedXmlNodeAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The value provided for one of the HTTP headers was not in the correct format.":
					return new InvalidHeaderValueAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The value provided for one of the XML nodes in the request body was not in the correct format.":
					return new InvalidXmlNodeValueAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "A required query parameter was not specified for this request.":
					return new MissingRequiredQueryParameterAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "One of the query parameters specified in the request URI is not supported.":
					return new UnsupportedQueryParameterAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "An invalid value was specified for one of the query parameters in the request URI.":
					return new InvalidQueryParameterValueAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "A query parameter specified in the request URI is outside the permissible range.":
					return new OutOfRangeQueryParameterValueAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The url in the request could not be parsed.":
					return new RequestUrlFailedToParseAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The requested URI does not represent any resource on the server.":
					return new InvalidUriAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The HTTP verb specified was not recognized by the server.":
					return new InvalidHttpVerbAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The key for one of the metadata key-value pairs is empty.":
					return new EmptyMetadataKeyAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The specified XML is not syntactically valid.":
					return new InvalidXmlDocumentAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The MD5 value specified in the request did not match the MD5 value calculated by the server.":
					return new Md5MismatchAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The MD5 value specified in the request is invalid. The MD5 value must be 128 bits and Base64-encoded.":
					return new InvalidMd5AzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "One of the request inputs is out of range.":
					return new OutOfRangeInputAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The authentication information was not provided in the correct format. Verify the value of Authorization header.":
					return new InvalidAuthenticationInfoAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "One of the request inputs is not valid.":
					return new InvalidInputAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The specified metadata is invalid. It includes characters that are not permitted.":
					return new InvalidMetadataAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The size of the specified metadata exceeds the maximum size permitted.":
					return new MetadataTooLargeAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "Server failed to authenticate the request. Make sure the value of the Authorization header is formed correctly including the signature.":
					return new AuthenticationFailedAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "Read-access geo-redundant replication is not enabled for the account.":
					return new InsufficientAccountPermissionsForReadAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "Write operations to the secondary location are not allowed.":
					return new InsufficientAccountPermissionsForWriteAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The specified resource does not exist.":
					return new ResourceNotFoundAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The specified account is disabled.":
					return new AccountIsDisabledAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The resource doesn't support the specified HTTP verb.":
					return new UnsupportedHttpVerbAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The specified account already exists.":
					return new AccountAlreadyExistsAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The specified account is in the process of being created.":
					return new AccountBeingCreatedAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The account being accessed does not have sufficient permissions to execute this operation.":
					return new InsufficientAccountPermissionsForExecuteAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The Content-Length header was not specified.":
					return new MissingContentLengthHeaderAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The condition specified in the conditional header(s) was not met for a write operation.":
					return new ConditionNotMetForWriteAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "Multiple condition headers are not supported.":
					return new MultipleConditionHeadersNotSupportedAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The size of the request body exceeds the maximum size permitted.":
					return new RequestBodyTooLargeAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The range specified is invalid for the current size of the resource.":
					return new InvalidRangeAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The server encountered an internal error. Please retry the request.":
					return new InternalErrorAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The operation could not be completed within the permitted time.":
					return new OperationTimedOutAzureException(requestId, statusCode, statusDescription, baseException);
				 
				case "The server is currently unable to receive requests. Please retry your request.":
					return new ServerBusyAzureException(requestId, statusCode, statusDescription, baseException);
				
			default:
				return new UnrecognizedAzureException(requestId, statusCode, statusDescription, baseException);
		}
	}

}

	///
	///<summary>
	///Represents a 'ConditionNotMetForRead' error response from the Azure Storage Service API
	///</summary>
	public class ConditionNotMetForReadAzureException : AzureException
    {
        public ConditionNotMetForReadAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'MissingRequiredHeader' error response from the Azure Storage Service API
	///</summary>
	public class MissingRequiredHeaderAzureException : AzureException
    {
        public MissingRequiredHeaderAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'MissingRequiredXmlNode' error response from the Azure Storage Service API
	///</summary>
	public class MissingRequiredXmlNodeAzureException : AzureException
    {
        public MissingRequiredXmlNodeAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'UnsupportedHeader' error response from the Azure Storage Service API
	///</summary>
	public class UnsupportedHeaderAzureException : AzureException
    {
        public UnsupportedHeaderAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'UnsupportedXmlNode' error response from the Azure Storage Service API
	///</summary>
	public class UnsupportedXmlNodeAzureException : AzureException
    {
        public UnsupportedXmlNodeAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'InvalidHeaderValue' error response from the Azure Storage Service API
	///</summary>
	public class InvalidHeaderValueAzureException : AzureException
    {
        public InvalidHeaderValueAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'InvalidXmlNodeValue' error response from the Azure Storage Service API
	///</summary>
	public class InvalidXmlNodeValueAzureException : AzureException
    {
        public InvalidXmlNodeValueAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'MissingRequiredQueryParameter' error response from the Azure Storage Service API
	///</summary>
	public class MissingRequiredQueryParameterAzureException : AzureException
    {
        public MissingRequiredQueryParameterAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'UnsupportedQueryParameter' error response from the Azure Storage Service API
	///</summary>
	public class UnsupportedQueryParameterAzureException : AzureException
    {
        public UnsupportedQueryParameterAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'InvalidQueryParameterValue' error response from the Azure Storage Service API
	///</summary>
	public class InvalidQueryParameterValueAzureException : AzureException
    {
        public InvalidQueryParameterValueAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'OutOfRangeQueryParameterValue' error response from the Azure Storage Service API
	///</summary>
	public class OutOfRangeQueryParameterValueAzureException : AzureException
    {
        public OutOfRangeQueryParameterValueAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'RequestUrlFailedToParse' error response from the Azure Storage Service API
	///</summary>
	public class RequestUrlFailedToParseAzureException : AzureException
    {
        public RequestUrlFailedToParseAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'InvalidUri' error response from the Azure Storage Service API
	///</summary>
	public class InvalidUriAzureException : AzureException
    {
        public InvalidUriAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'InvalidHttpVerb' error response from the Azure Storage Service API
	///</summary>
	public class InvalidHttpVerbAzureException : AzureException
    {
        public InvalidHttpVerbAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'EmptyMetadataKey' error response from the Azure Storage Service API
	///</summary>
	public class EmptyMetadataKeyAzureException : AzureException
    {
        public EmptyMetadataKeyAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'InvalidXmlDocument' error response from the Azure Storage Service API
	///</summary>
	public class InvalidXmlDocumentAzureException : AzureException
    {
        public InvalidXmlDocumentAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'Md5Mismatch' error response from the Azure Storage Service API
	///</summary>
	public class Md5MismatchAzureException : AzureException
    {
        public Md5MismatchAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'InvalidMd5' error response from the Azure Storage Service API
	///</summary>
	public class InvalidMd5AzureException : AzureException
    {
        public InvalidMd5AzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'OutOfRangeInput' error response from the Azure Storage Service API
	///</summary>
	public class OutOfRangeInputAzureException : AzureException
    {
        public OutOfRangeInputAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'InvalidAuthenticationInfo' error response from the Azure Storage Service API
	///</summary>
	public class InvalidAuthenticationInfoAzureException : AzureException
    {
        public InvalidAuthenticationInfoAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'InvalidInput' error response from the Azure Storage Service API
	///</summary>
	public class InvalidInputAzureException : AzureException
    {
        public InvalidInputAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'InvalidMetadata' error response from the Azure Storage Service API
	///</summary>
	public class InvalidMetadataAzureException : AzureException
    {
        public InvalidMetadataAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'MetadataTooLarge' error response from the Azure Storage Service API
	///</summary>
	public class MetadataTooLargeAzureException : AzureException
    {
        public MetadataTooLargeAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'AuthenticationFailed' error response from the Azure Storage Service API
	///</summary>
	public class AuthenticationFailedAzureException : AzureException
    {
        public AuthenticationFailedAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'InsufficientAccountPermissionsForRead' error response from the Azure Storage Service API
	///</summary>
	public class InsufficientAccountPermissionsForReadAzureException : AzureException
    {
        public InsufficientAccountPermissionsForReadAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'InsufficientAccountPermissionsForWrite' error response from the Azure Storage Service API
	///</summary>
	public class InsufficientAccountPermissionsForWriteAzureException : AzureException
    {
        public InsufficientAccountPermissionsForWriteAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'ResourceNotFound' error response from the Azure Storage Service API
	///</summary>
	public class ResourceNotFoundAzureException : AzureException
    {
        public ResourceNotFoundAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'AccountIsDisabled' error response from the Azure Storage Service API
	///</summary>
	public class AccountIsDisabledAzureException : AzureException
    {
        public AccountIsDisabledAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'UnsupportedHttpVerb' error response from the Azure Storage Service API
	///</summary>
	public class UnsupportedHttpVerbAzureException : AzureException
    {
        public UnsupportedHttpVerbAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'AccountAlreadyExists' error response from the Azure Storage Service API
	///</summary>
	public class AccountAlreadyExistsAzureException : AzureException
    {
        public AccountAlreadyExistsAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'AccountBeingCreated' error response from the Azure Storage Service API
	///</summary>
	public class AccountBeingCreatedAzureException : AzureException
    {
        public AccountBeingCreatedAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'InsufficientAccountPermissionsForExecute' error response from the Azure Storage Service API
	///</summary>
	public class InsufficientAccountPermissionsForExecuteAzureException : AzureException
    {
        public InsufficientAccountPermissionsForExecuteAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'MissingContentLengthHeader' error response from the Azure Storage Service API
	///</summary>
	public class MissingContentLengthHeaderAzureException : AzureException
    {
        public MissingContentLengthHeaderAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'ConditionNotMetForWrite' error response from the Azure Storage Service API
	///</summary>
	public class ConditionNotMetForWriteAzureException : AzureException
    {
        public ConditionNotMetForWriteAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'MultipleConditionHeadersNotSupported' error response from the Azure Storage Service API
	///</summary>
	public class MultipleConditionHeadersNotSupportedAzureException : AzureException
    {
        public MultipleConditionHeadersNotSupportedAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'RequestBodyTooLarge' error response from the Azure Storage Service API
	///</summary>
	public class RequestBodyTooLargeAzureException : AzureException
    {
        public RequestBodyTooLargeAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'InvalidRange' error response from the Azure Storage Service API
	///</summary>
	public class InvalidRangeAzureException : AzureException
    {
        public InvalidRangeAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'InternalError' error response from the Azure Storage Service API
	///</summary>
	public class InternalErrorAzureException : AzureException
    {
        public InternalErrorAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'OperationTimedOut' error response from the Azure Storage Service API
	///</summary>
	public class OperationTimedOutAzureException : AzureException
    {
        public OperationTimedOutAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

		///
	///<summary>
	///Represents a 'ServerBusy' error response from the Azure Storage Service API
	///</summary>
	public class ServerBusyAzureException : AzureException
    {
        public ServerBusyAzureException(string requestId, HttpStatusCode statusCode, string statusDescription, WebException baseException)
            : base(requestId, statusCode, statusDescription, baseException) { }
    }

	}