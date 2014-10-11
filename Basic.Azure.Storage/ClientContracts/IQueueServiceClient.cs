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
        #region Queue Operations

        void CreateQueue(string queueName, Dictionary<string, string> metadata = null);
        Task CreateQueueAsync(string queueName, Dictionary<string, string> metadata = null);

        void DeleteQueue(string queueName);
        Task DeleteQueueAsync(string queueName);

        GetQueueMetadataResponse GetQueueMetadata(string queueName);
        Task<GetQueueMetadataResponse> GetQueueMetadataAsync(string queueName);

        void SetQueueMetadata(string queueName, Dictionary<string, string> metadata);
        Task SetQueueMetadataAsync(string queueName, Dictionary<string, string> metadata);

        #endregion

        #region Message Operations

        void PutMessage(string queueName, string messageData, int? visibilityTimeout = null, int? messageTtl = null);
        Task PutMessageAsync(string queueName, string messageData, int? visibilityTimeout = null, int? messageTtl = null);

        #endregion



    }
}
