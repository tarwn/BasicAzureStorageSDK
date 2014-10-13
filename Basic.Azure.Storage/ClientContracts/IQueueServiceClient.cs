using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.QueueService.AccountOperations;
using Basic.Azure.Storage.Communications.QueueService.MessageOperations;
using Basic.Azure.Storage.Communications.QueueService.QueueOperations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.ClientContracts
{
    public interface IQueueServiceClient
    {
        #region Account Operations

        ListQueuesResponse ListQueues(string prefix = "", int maxResults = 5000, string marker = null, bool includeMetadata = false);
        Task<ListQueuesResponse> ListQueuesAsync(string prefix = "", int maxResults = 5000, string marker = null, bool includeMetadata = false);

        void SetQueueServiceProperties(StorageServiceProperties expectedServiceProperties);
        Task SetQueueServicePropertiesAsync(StorageServiceProperties expectedServiceProperties);

        #endregion

        #region Queue Operations

        void CreateQueue(string queueName, Dictionary<string, string> metadata = null);
        Task CreateQueueAsync(string queueName, Dictionary<string, string> metadata = null);

        void DeleteQueue(string queueName);
        Task DeleteQueueAsync(string queueName);

        GetQueueMetadataResponse GetQueueMetadata(string queueName);
        Task<GetQueueMetadataResponse> GetQueueMetadataAsync(string queueName);

        void SetQueueMetadata(string queueName, Dictionary<string, string> metadata);
        Task SetQueueMetadataAsync(string queueName, Dictionary<string, string> metadata);

        GetQueueACLResponse GetQueueACL(string queueName);
        Task<GetQueueACLResponse> GetQueueACLAsync(string queueName);

        void SetQueueACL(string queueName, List<SignedIdentifier> signedIdentifiers);
        Task SetQueueACLAsync(string queueName, List<SignedIdentifier> signedIdentifiers);

        #endregion

        #region Message Operations

        void PutMessage(string queueName, string messageData, int? visibilityTimeout = null, int? messageTtl = null);
        Task PutMessageAsync(string queueName, string messageData, int? visibilityTimeout = null, int? messageTtl = null);

        GetMessagesResponse GetMessages(string queueName, int numofMessages = 1, int visibilityTimeout = 30, int? messageTtl = null);
        Task<GetMessagesResponse> GetMessagesAsync(string queueName, int numofMessages = 1, int visibilityTimeout = 30, int? messageTtl = null);

        PeekMessagesResponse PeekMessages(string queueName, int numofMessages = 1, int visibilityTimeout = 30, int? messageTtl = null);
        Task<PeekMessagesResponse> PeekMessagesAsync(string queueName, int numofMessages = 1, int visibilityTimeout = 30, int? messageTtl = null);

        void DeleteMessage(string queueName, string messageId, string popReceipt);
        Task DeleteMessageAsync(string queueName, string messageId, string popReceipt);

        void ClearMessages(string queueName);
        Task ClearMessagesAsync(string queueName);

        UpdateMessageResponse UpdateMessage(string queueName, string messageId, string popReceipt, int visibilityTimeout = 30, string messageData = null);
        Task<UpdateMessageResponse> UpdateMessageAsync(string queueName, string messageId, string popReceipt, int visibilityTimeout = 30, string messageData = null);

        #endregion



        
    }
}
