BasicAzureStorageSDK
====================

This is the beginning of a .Net SDK for accessing the Azure Storage API. It targets the latest version 
that is also supported by the local emulator (not counting preview versions).

Current Supported API Version: 2012-02-12

I have not decided yet whether this will be a partial, reference implementation or a full implementation.

Motivation
====================

As I watched (and lived through, and tried to figure out) the evolution of the official 
Azure Storage SDK for .Net, I've continued to be annoyed by several things:

1) Pseudo-State: The attempted object-ification of a REST service (adding state where there is none)

2) Dueling Documentation: The mismatch in terminology and operations between the API and SDK

3) Emulator Support: The inclusion of features that cannot be used for local development due to 
   lack of support in the emulator

4) Information: The generic way exceptions are handled and thrown compared to the detail available from the API

5) Information: The lack of informaiton about failed retries

6) Readability.

7) Testability: There is not a clear interface or set of overrides I can do to execute local unit tests.

8) Testability: The internal tests for the SDK use logic to mangle HTTP requests, are long and rambling, and have
   actually been removed from the most recent version I looked at on github (3.0).

Some of these have improved with the later versions and with some of the tracing methods in later versions, but
some have remained underlying concepts (such as trying to use stateful objects to represent a service).

I've written an implementation of the Azure API before, but I wanted to start writing one that:

1) Matches the API. The documentation should exist only to help understand how to do operations from the API
   in this library, setup things like the storage settings or error handling, and call out exceptions to the API.

2) Treats the service as a service. If you want to consume the service and add some pseudo form of state, do so. 
   But the SDk should not force that on you.
   
3) Provides every scrap of error information possible to help the developer or maintainer do their job.

4) Provides underlying informaiton about retry rates and retries that were later succesful

5) Matches the API internally as well. Finding the implementation of an API operation should not require opening 
   15 files.
   
6) Has a clear set of tests that serves almost as a bullet point list of how the API works for each operation

7) Is easily mockable for unit testing code that interacts with the library.

8) Does not force you to use asynchronous patterns throughout your codebase (except where the API requires them)

9) Requires no magic. There are no exposed properties that are only correct after another call is made, no
   enumerated values that are not enums, and when a parametr has character or length restrictions, the library
   should be smart enough to tell you about them.

Currently the library does not have interfaces for the top-level client objects for #7, and retries are only
communicated upward at the end of a failed operation rather than providing hooks to gather informaiotn on
each individual failed retry. Restrictions on length and character usage have also not been added yet.

Geography
====================

The library tries to stick as close to the API structure and naming as possible. 

Using the Library
--------------------

These are the objects you would access while using the library.

_Clients_

- BlobStorageClient - operations for Blob Storage Service - http://msdn.microsoft.com/en-us/library/azure/dd135733.aspx
- QueueServiceClient - operations for Queue Service - http://msdn.microsoft.com/en-us/library/azure/dd179363.aspx
- TableServiceClient - operations for Table Service - http://msdn.microsoft.com/en-us/library/azure/dd179423.aspx

Operations on each of these clients are named the same as they are in the corresponding API documentation. 
Parameters that are required in the documentation are required by operations in the client, parameters that
are optional in the documentation are optional in the client. If there is only a limited set of input options
allowed, an enum will exist that has those options.

Currently PutBlob is an exception, as it has been split into a PutBlockBlob and PutPageBlob so there is no 
confusion over whether a parameter is optional because it is optional for the blob type you are operating on
or because it is only valid for the opposite type.

_Responses_

Storage API calls that returns responses will have a matching response object defined in the library that
is named after the operation. Executing a CreateContainer operation, for instance, will return a 
CreateContainerResponse.

_Exceptions_

There is a specific exception for each error listed in the documented API error tables.

- Common Service Errors
- Blob Service Errors
- Queue Service Errors
- Table Service Errors

Reference: http://msdn.microsoft.com/en-us/library/azure/dd179382.aspx

They all share a common base AzureException, so you have the option of handling all Azure Exceptions in a single
catch statement, or add logic for specific errors (such as BlobNotFoundAzureException).

_Retries_

There is a default retry Policy currently in place that will be exposed later. When this retry policy is exhausted,
it wraps the final specific exception in a RetriedException so you have both the final exception details as well
as information on how many times the operation was tried before giving up.

Internal Geography
-------------------

Expanding on the usage above, the internals try to follow a consistent pattern to make it easy to match up with
the API documentation.

_Services and Operation Requests and Responses_

The services folder structure follows the documentation folder structure. 

See http://msdn.microsoft.com/en-us/library/azure/dd179355.aspx

Each service type (Blob, Queue, table) has a corresponding folder in Communications. They then have the same 
sub-folders as the documentation. 

Example: Put Blob
Library Location: /Communications/BlobService/BlobOperations/PutBlobRequest
Documentation: Blob Service - Operations on Blobs - Put Blob

I'm not sure if MSDN renamed the "Operations" level or if I was inconsistent about it, the documentation has 
moved since I initially started this.

_Exceptions_

Exceptions live at Communications/Exceptions. They are generated from the documentated tables of errors in the API,
using regular expressions to tweak the output and then T4 to generate the actual classes.

_Core Logic_

The core logic includes the RequestBase, the Response wrapper that wraps around the specific resposne payload 
expected for an operation, the RetriedException that is returned when we give up after exhausting the retry policy,
and various constants for header values and such, the logic to create signed authorization headers, etc.

Technical Details
====================

The two biggest design changes I am going to make as I continue to work on this are around asynchronous operations.

TPL Tasks
-----------

Currently the internal implementation uses TPL Tasks because I was familiar with this method from building an 
earlier version and needed more production experience using async/await.

Synchronous Clients
--------------------

The 3 clients (Blob, Table, Client) currently only expose synchronous methods. Surfacing asynchronous requests
will be added as well (the underlying logic already supports it). It was built this way because plumbing out the
early calls to each of the services had enough complexity without also trying to write tests that were managing
asynchronous calls.
