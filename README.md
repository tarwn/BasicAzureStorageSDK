BasicAzureStorageSDK
====================

This is .Net SDK designed to closely match the Azure Storage API. The operations align with the API documentation,
it supports only features that can be tested locally against the storage emulator, and provides interfaces and
no internal methods.

Because "use a real storage account" is not good advice.

Current Supported API Version: 2012-02-12 - This will be caught up after I have finished the current round of
additions.


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

9) Requires no magic. There are no exposed properties that are only correct after another call is made, no
   enumerated values that are not enums, no properties that actually call the API behind the scenes,  and when a
   parametr has character or length restrictions, the library should be smart enough to tell you about them.

Currently retries are only communicated upward at the end of a failed operation rather than providing hooks to 
gather information on each individual failed retry. Restrictions on length and character usage have also not been 
added yet.

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

*PutBlob* Currently PutBlob is an exception, as it has been split into a PutBlockBlob and PutPageBlob. In the API,
some of the parameters are truly optional and some are only optional depending on which flavor you are uploading,
so I chose to split it into two methods so that only the truly optional parameters would be optional and the ones
that are type dependeant are not available for the opposite call.

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

In Progress
====================

There are a number of the design considerations above that are still in progress.

Synchronous Clients
--------------------

The clients (Blob, Table, Client) started out exposing only synchronous methods. Surfacing asynchronous methods
is in progress.

Interfaces
--------------------

The interfaces (/ClientContracts/*) are still in progress. The QueueService contract has been added as I add more
methods to the QueueServiceClient, the other two will be added when I return to add more calls to them.


Implemented Methods
====================

This section will list all of the available methods from the documentation, as of 2014-10-13, and whether they have 
been implemented yet.

- Queue is done for 2012-02-12
- Table is barely started
- Blob is barely started
- File is not present at all

Queue Service - QueueServiceClient: IQueueServiceClient
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

Table Service - TableServiceClient: ITableServiceClient
-----------------------------------------------------------

Account Operations

- Set Table Service Properties - No
- Get Table Service Properties - No
- Preflight Table Request - No
- Get Table Service Stats - No

Table Operations

- Query Tables - No
- Create Table - Yes
- Delete Table - No
- Get Table ACL - No
- Set Table ACL - No

Entity Operations

- Query Entities - No
- Insert Entity - No
- Update Entity - No
- Merge Entity - No
- Delete Entity - No
- Insert or Replace Entity - No
- Insert or Merge Entity - No

Blob Service - BlobServiceClient: IBlobServiceClient
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
- Get Container Metadata - No
- Set Container Metadata - No
- Get Container ACL - No
- Set Container ACL - No
- Delete Container - No
- Lease Container - No
- List Blobs - No

Blob Operations

- Put Blob - Yes - two flavors (see notes above): PutBlockBlob and PutPageBlob
- Get Blob - No
- Get Blob Properties - No
- Set Blob Properties - No
- Get Blob Metadata - No
- Set Blob Metadata - No
- Lease Blob - No
- Snapshot Blob - No
- Copy Blob - No
- Abort Copy Blob - No
- Delete Blob - No

Block Blob Operations

- Put Block - No
- Put Block List - No
- Get Block List - No

Page Blob Operations

- Put Page - No
- Get Page Ranges - No

File Service - N/A
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
