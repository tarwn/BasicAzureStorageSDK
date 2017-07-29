BasicAzureStorageSDK
====================

This is a .Net SDK designed to closely match the Azure Storage API, focused mostly on Queue and Blob storage 
(with limited support for Table). The operations align with the API documentation, it supports only features 
that can be tested locally against the storage emulator, and provides interfaces and no internal methods.

* .Net Framework Version: 4.5 or newer
* Azure Storage API Version: 2013-08-15
* NuGet Dependencies: Netwonsoft.Json, TransientFaultHandling.Core

Supported Operations:

* Queue Service - 15/17 (missing preflight + service stats)
* Blob Service - 20/31 (missing account-level calls, page, append-only)
* Table Service - 7 + 2 halves/16
* File Service - 0/19

The library includes:

* Interfaces or overridable methods, for easier unit testing
* Built-in Retry Policies
* Strongly-typed Exceptions for all service errors, ex: BlobNotFoundAzureException
* Service clients (blob, queue, table) that closely match the Storage API Documentation
* Helper methods for automatically managing large block blobs
* Sync and Async implementations of every method
* Unit and Integration tests for every service method (verified against the Storage SDK)

License
====================

See LICENSE.txt

Getting Started
====================

There are 2 key ingredients in every call to Azure Storage, the StorageAccountSettings and a ServiceClient.

Here's a demo of creating a queue, adding an item to it, and reading that item:

	var accountSettings = StorageAccountSettings.Parse("UseDevelopmentStorage=true");
	var client = new QueueServiceClient(accountSettings);

	client.CreateQueue("myawesomequeue");

	client.PutMessage("myawesomequeue", "my test message!");

	var response = client.GetMessages("myawesomequeue", 1);

	//do something with response.Messages[0]

	client.DeleteMessage("myawesomequeue", response.Messages[0].Id, response.Messages[0].PopReceipt);

All methods have both a synchronous and Async version, so this can also be written like so:

	var accountSettings = StorageAccountSettings.Parse("UseDevelopmentStorage=true");
	var client = new QueueServiceClient(accountSettings);

	await client.CreateQueueAsync("myawesomequeue");

	await client.PutMessageAsync("myawesomequeue", "my test message!");

	var response = await client.GetMessagesAsync("myawesomequeue", 1);

	//do something with response.Messages[0]

	await client.DeleteMessageAsync("myawesomequeue", response.Messages[0].Id, response.Messages[0].PopReceipt);

The methods in the QueueServiceClient match the [API documentation for the Queue service](http://msdn.microsoft.com/en-us/library/azure/dd179363.aspx) closely.

## StorageAccountSettings

You can define the storage account settings with either the constructor or by feeding in an [Azure Storage 
connection string](https://azure.microsoft.com/en-us/documentation/articles/storage-configure-connection-string/) like you would the official SDK:

	// some examples with connection strings
	var accountSettings = StorageAccountSettings.Parse("UseDevelopmentStorage=true");
	var accountSettings = StorageAccountSettings.Parse("AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;");
	var accountSettings = StorageAccountSettings.Parse("AccountName=some-account-name;AccountKey=some-account-key;");

	// or the constructor
	var accountSettings = new StorageAccountSettings("devstoreaccount1", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");
	var accountSettings = new StorageAccountSettings("some-account-name", "some-account-key");

Support is included for changing HTTP/HTTPS, providing alternate blob/table/queue endpoints, and address suffixes 
other than "core.windows.net". 

(See [StorageAccountSettingsTests](https://github.com/tarwn/BasicAzureStorageSDK/blob/master/Basic.Azure.Storage.Tests/StorageAccountSettingsTests.cs) for test examples of the different connection strings that can be parsed).


Using the library
====================

There is not a published NuGet package for this library (coming soon).

To add this to a project:

* Clone the Download [NuGet.exe](https://dist.nuget.org/index.html)
* Build the project in Release mode
* Run `nuget.exe pack`
* Copy the resulting nuget package to a folder in your solution and add that folder as a nuget repository

Geography
====================

The library tries to stick as close to the API structure and naming as possible.

Using the Library
--------------------

These are the objects you would access while using the library.

### Clients

- BlobStorageClient - operations for Blob Storage Service - http://msdn.microsoft.com/en-us/library/azure/dd135733.aspx
- QueueServiceClient - operations for Queue Service - http://msdn.microsoft.com/en-us/library/azure/dd179363.aspx
- TableServiceClient - operations for Table Service - http://msdn.microsoft.com/en-us/library/azure/dd179423.aspx

Operations on each of these clients are named the same as they are in the corresponding API documentation.
Parameters that are required in the documentation are required by operations in the client, parameters that
are optional in the documentation are optional in the client. If there is only a limited set of input options
allowed, an enum will exist that has those options.

### Differences

For the most part, each API call will have 1 matching Request object. However, there are a few API calls that
are actually multiple actions in a single call, and for those I have decided to make alter the pattern so the
end consumer only has to deal with what is required/allowed for a specific action of the call rather than
all possible inputs for all possible actions on one request.

**PutBlob** has been split into a PutBlockBlob and PutPageBlob. In the API, some of the parameters are truly
optional and some are only optional depending on which flavor you are uploading, so I chose to split it into
two methods so that only the truly optional parameters would be optional and the ones that are type dependeant
are not available for the opposite call.

**LeaseBlob** Has 5 actions with different allowed and required inputs and different expected responses.
I have split this into 5 calls because I would prefer to make it clear and simple which arguments are
necessary for each call, even if it does somewhat break the model of 1 API call = 1 Request object.

**LeaseContainer** Has 5 actions with different allowed and required inputs and different expected responses.
I have split this into 5 calls because I would prefer to make it clear and simple which arguments are
necessary for each call, even if it does somewhat break the model of 1 API call = 1 Request object.

Note: This is an area that I find questionable in the API. I'm not sure why they chose to embed multiple
actions into common requests like this, I think they generally did a good idea of making the API clear and
these points stand out in contradiction to the rest.

### Responses

Storage API calls that returns responses will have a matching response object defined in the library that
is named after the operation. Executing a CreateContainer operation, for instance, will return a
CreateContainerResponse.

### Exceptions

There is a specific exception for each error listed in the documented API error tables.

- Common Service Errors
- Blob Service Errors
- Queue Service Errors
- Table Service Errors

Reference: http://msdn.microsoft.com/en-us/library/azure/dd179382.aspx

They all share a common base AzureException, so you have the option of handling all Azure Exceptions in a single
catch statement, or add logic for specific errors (such as BlobNotFoundAzureException).

### Retries

There is a default retry Policy currently in place that will be exposed later. When this retry policy is exhausted,
it wraps the final specific exception in a RetriedException so you have both the final exception details as well
as information on how many times the operation was tried before giving up.

Internal Geography
-------------------

The layout of the code internally follows some patterns as well.

- /ClientContracts - the interfaces for the Blob/Table/Queue client objects above
- /Communications/BlobService - the blob service operations
- /Communications/Common - common data objects and enums used in API operations
- /Communications/Core - the core logic for Requests and Responses distilled into one place so Request/Response for Operations only reflect the specific requirements for the operation
- /Communications/QueueService - the Queue Service operations
- /Communications/ServiceExceptions - concrete exception for each documented service exception, a base AzureException, and an AzureResponseParseException
- /Communications/TableService - the Table Service operations
- /Communications/Utility - utility classes for parsing and formatting of data used by Request and Response objects

### Service Folder Structures

The services folder structure follows the documentation folder structure, with a single high level folder for the
service, sub-folders for the categories of operations, and then Request and Response objects named the same as the
API operations.

Example: Create Container - http://msdn.microsoft.com/en-us/library/windowsazure/dd179468.aspx
Library Location: /Communications/BlobService/ContainerOperations/CreateContainerRequest
Documentation: Blob Service / Operations on Containers / Create Contaner

_Note: I'm not sure if MSDN renamed the "Operations" level or if I was inconsistent about it, the documentation has
moved since I initially started this._

### Service Exceptions

Service Exceptions are generated from the documentated tables of errors in the API, using regular expressions to
tweak the output and then T4 to generate the actual classes. They have a common base class, giving you the freedom
to catch exceptions as specifically or generally as you want.

These exceptions also include all of the details available. The API often provides more details for errors than are
surfaced in the standard Azure Storage SDK; these exceptions surface all of that additional information.

### Retried Exceptions

When the retry policy is exhausted, the final exception is wrapped in a RetriedException to provide information that
the Operation was retried multiple times, including the number of times it was tried. This is often one of the first
things that Azure support asks. later I may also add the full collection of exceptions that happened, as there is at
least one outstanding issue around popreceipts that requires you to know about the first error that occurred.

### Core Logic

The core logic includes the RequestBase, the Response wrapper that wraps around the specific resposne payload
expected for an operation, the RetriedException that is returned when we give up after exhausting the retry policy,
and various constants for header values and such, the logic to create signed authorization headers, etc.

Implemented Methods
====================

This section will list all of the available methods from the documentation, as of 2014-10-13, and whether they have
been implemented yet.

- Queue is mostly done - 15/17 operations
- Table is started - 7 + 2 halves/16 operations
- Blob is pretty far - 20/31 operations
- File is not present at all

Queue Service - 15/17 - QueueServiceClient: IQueueServiceClient
-----------------------------------------------------------

Account Operations

- List Queues - Complete - does not automatically download with continuation markers
- Set Queue Service Properties - Complete - does not include CORS additions from 2013
- Get Queue Servuce Properties - Complete - does not include CORS additions from 2013
- Preflight Queue Request - No
- Get Queue Service States - No

Queue Operations

- Create Queue - Yes
- Delete Queue - Yes
- Get Queue Metadata - Yes
- Set Queue Metadata - Yes
- Get Queue ACL - Yes
- Set Queue ACL - Yes

Message Operations

- Put Message - Yes
- Get Messages - Yes
- Peek Messages - Yes
- Delete Message - Yes
- Clear Messages - Yes - Does not auto-retry the 500 Operation Timeout yet
- Update Message - Yes

Blob Service - 20/31 - BlobServiceClient: IBlobServiceClient
-----------------------------------------------------------

Account Operations

- List Containers - No
- Set Blob Service Properties - No
- Get Blob Service Properties - No
- Preflight Blob Request - No
- Get Blob Service States - No

Container Operations

- Create Container - Yes
- Get Container Properties - Yes
- Get Container Metadata - Yes
- Set Container Metadata - Yes
- Get Container ACL - Yes
- Set Container ACL - Yes
- Delete Container - Yes
- Lease Container - Yes
- List Blobs - Yes

Blob Operations

- Put Blob - Yes - two flavors (see notes above): PutBlockBlob and PutPageBlob
- Get Blob - Yes
- Get Blob Properties - Yes
- Set Blob Properties - No
- Get Blob Metadata - Yes
- Set Blob Metadata - Yes
- Lease Blob - Yes
- Snapshot Blob - No
- Copy Blob - Yes
- Abort Copy Blob - No
- Delete Blob - Yes

Block Blob Operations

- Put Block - Yes
- Put Block List - Yes
- Get Block List - Yes

Page Blob Operations

- Put Page - No
- Get Page Ranges - No

Append Blob Operations

- Append Block - No


Table Service - 7 + 2 halves/16 - TableServiceClient: ITableServiceClient
-----------------------------------------------------------

Account Operations

- Set Table Service Properties - No
- Get Table Service Properties - No
- Preflight Table Request - No
- Get Table Service Stats - No

Table Operations

- Query Tables - Basic (no OData support yet)
- Create Table - Yes
- Delete Table - No
- Get Table ACL - No
- Set Table ACL - No

Entity Operations

- Query Entities - Flavor A (QueryEntity by PartKey/RowKey) - Yes
- Query Entities - Flavor B (QueryEntities by OData $filter) - No
- Insert Entity - Yes
- Update Entity - Yes
- Merge Entity - Yes
- Delete Entity - Yes
- Insert or Replace Entity - Yes
- Insert or Merge Entity - Yes

File Service - 0/19 - N/A
-----------------------------------------------------------

Account Operations

- List Shares - No

Share Operations

- Create Share - No
- Get Share Properties - No
- Get Share Metadata - No
- Set Share Metadata - No
- Delete Share - No

Directory Operations

- List Directories and Files - No
- Create Directory - No
- Get Directory Properties - No
- Delete Directory - No

File Operations

- Create File - No
- Get File - No
- Get File Properties - No
- Set File Properties - No
- Put Range - No
- List Ranges - No
- Get File Metadata - No
- Set File Metadata - No
- Delete File - No


Motivation
====================

<blockquote>Ask me about the Azure Storage SDK...</blockquote>

As I watched (and lived through, and tried to figure out) the evolution of the official
Azure Storage SDK for .Net, I've continued to be annoyed by several things:

1) Pseudo-State: The attempted object-ification of a REST service (adding state where there is none)

2) Dueling Documentation: The mismatch in terminology and operations between the API and SDK

3) Emulator Support: The inclusion of features that cannot be used for local development due to
   lack of support in the emulator

4) Information: The generic way exceptions are handled and thrown compared to the detail available from the API
   _Note: There is extended info in the Storage SDK exception, you just have to dig to find it and it has moved
          a couple times_

5) Information: The lack of information about failed retries

6) Readability.

7) Testability: There is not a clear interface to mock or fake for tests.

8) Testability: The internal tests for the SDK use logic to mangle HTTP requests, are long and rambling, and have
   been removed from some versions (3.0).

Some of these have improved with the later versions and with some of the tracing methods in later versions, but
some have remained underlying concepts (such as trying to use stateful objects to represent a service).

I've written an implementation of the Azure API before, but I wanted to start writing one that:

1) Matches the API. The documentation should exist only to describe the naming gap between the two, the few
   places where the local library diverges from the API, how to setup things like the storage settings and error
   handling, and how exceptions map to the API.

2) Treats the service as a service. If you want to consume the service and add some pseudo form of state, do so.
   The SDK should not force that on you.

3) Provides every scrap of error information possible to help the developer or maintainer do their job.

4) Provides underlying information about retry rates and retries that were later succesful

5) Matches the API internally as well. Finding the implementation details of an API operation should not require
   opening 15 files.

6) Has a clear set of tests that serves almost as a bullet point list of how the API works for each operation

7) Is easily mockable for unit testing code that interacts with the library.

8) Does not force you to use asynchronous patterns throughout your codebase (except where the API requires them)
   _Note: The standard Storage SDK doesn't force this, but many other libraries these days use only Async methods,
          assuming that converting an entire existing application to async/await is an easy process_

9) Requires no magic. There are no exposed properties that are only correct after another call is made, no
   enumerated values that are not enums, no properties that actually call the API behind the scenes,  and when a
   parameter has character or length restrictions, the library should be smart enough to tell you about them.

Currently retries are only communicated upward at the end of a failed operation rather than providing hooks to
gather information on each individual failed retry. Restrictions on length and character usage have also not been
added yet.
