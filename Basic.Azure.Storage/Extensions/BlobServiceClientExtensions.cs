using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.BlobService.BlobOperations;
using Basic.Azure.Storage.Communications.Utility;
using Basic.Azure.Storage.Extensions.Contracts;

namespace Basic.Azure.Storage.Extensions
{
    public class BlobServiceClientEx : BlobServiceClient, IBlobServiceClientEx
    {
        private readonly int _maxSingleBlobUploadSize;

        public int MaxSingleBlobUploadSize { get { return _maxSingleBlobUploadSize; } }

        public BlobServiceClientEx(StorageAccountSettings account, int maxSingleBlobUploadSize = BlobServiceConstants.MaxSingleBlobUploadSize)
            : base(account)
        {
            _maxSingleBlobUploadSize = maxSingleBlobUploadSize;
        }

        public IBlobOrBlockListResponseWrapper PutBlockBlobIntelligently(int blockSize,
            string containerName, string blobName, byte[] data, string leaseId,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null)
        {
            return RunSynchronously(async () =>
                await PutBlockBlobIntelligentlyAsync(blockSize, containerName, blobName, data, leaseId, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata));
        }
        public IBlobOrBlockListResponseWrapper PutBlockBlobIntelligently(int blockSize,
            string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null)
        {
            return RunSynchronously(async () =>
                await PutBlockBlobIntelligentlyAsync(blockSize, containerName, blobName, data, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata));
        }
        public async Task<IBlobOrBlockListResponseWrapper> PutBlockBlobIntelligentlyAsync(int blockSize,
            string containerName, string blobName, byte[] data, string leaseId,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null)
        {
            if (data.Length < MaxSingleBlobUploadSize)
            {
                return await PutBlockBlobAsync(containerName, blobName, data, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata, leaseId);
            }
            else
            {
                return await PutBlockBlobAsListAsync(blockSize, containerName, blobName, data, leaseId, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata);
            }
        }
        public async Task<IBlobOrBlockListResponseWrapper> PutBlockBlobIntelligentlyAsync(int blockSize,
            string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null)
        {
            var lease = await BlobLeaseMaintainer.LeaseNewOrExistingBlockBlobAndMaintainLease(this, containerName, blobName, 60 /* seconds */);

            var putResult = await PutBlockBlobIntelligentlyAsync(blockSize, containerName, blobName, data, lease.LeaseId, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata);

            await lease.StopMaintainingAndClearLease();

            return putResult;
        }

        public PutBlockListResponse PutBlockBlobAsList(int blockSize,
            string containerName, string blobName, byte[] data, string leaseId,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null)
        {
            return RunSynchronously(async () =>
                    await PutBlockBlobAsListAsync(blockSize, containerName, blobName, data, leaseId, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata));
        }
        public PutBlockListResponse PutBlockBlobAsList(int blockSize,
            string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null)
        {
            return RunSynchronously(async () =>
                    await PutBlockBlobAsListAsync(blockSize, containerName, blobName, data, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata));
        }
        public async Task<PutBlockListResponse> PutBlockBlobAsListAsync(int blockSize,
            string containerName, string blobName, byte[] data, string leaseId,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null)
        {
            var rangesAndBlockIds = GetBlockRangesAndIds(data.Length, blockSize);

            var putBlockRequests = rangesAndBlockIds
                .Select(blockInfo => GeneratePutBlockRequestAsync(containerName, blobName, data, blockInfo, leaseId))
                .ToList();

            var actualBlockIdList = GenerateBlockIdListFromRangesAndIds(rangesAndBlockIds);

            await Task.WhenAll(putBlockRequests);
            return await PutBlockListAsync(containerName, blobName, actualBlockIdList,
                    cacheControl, contentType, contentEncoding, contentLanguage, contentMD5, metadata, leaseId);
        }
        public async Task<PutBlockListResponse> PutBlockBlobAsListAsync(int blockSize,
            string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null)
        {
            var lease = await BlobLeaseMaintainer.LeaseNewOrExistingBlockBlobAndMaintainLease(this, containerName, blobName, 60 /* seconds */);

            var putResult = await PutBlockBlobAsListAsync(blockSize, containerName, blobName, data, lease.LeaseId, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata);

            await lease.StopMaintainingAndClearLease();

            return putResult;
        }

        private static List<ArrayRangeWithBlockIdString> GetBlockRangesAndIds(int arrayLength, int blockSize)
        {
            var blockRangesAndIds = new List<ArrayRangeWithBlockIdString>();

            var remainder = arrayLength % blockSize;
            var startOfRemainder = arrayLength - remainder - 1;

            for (var currentIndex = 0; currentIndex < arrayLength; currentIndex += blockSize)
            {
                blockRangesAndIds.Add(
                    new ArrayRangeWithBlockIdString
                    {
                        Id = GenerateBlockId(),
                        Offset = currentIndex,
                        Length = (currentIndex <= startOfRemainder ? blockSize : remainder)
                    }
                );
            }

            return blockRangesAndIds;
        }

        private static BlockListBlockIdList GenerateBlockIdListFromRangesAndIds(IEnumerable<ArrayRangeWithBlockIdString> rangesAndBlockIds)
        {
            var convertedBlockListBlockIds = rangesAndBlockIds
                .Select(blockInfo => new BlockListBlockId { Id = blockInfo.Id, ListType = PutBlockListListType.Uncommitted });

            return new BlockListBlockIdList(convertedBlockListBlockIds);
        }

        private async Task<PutBlockResponse> GeneratePutBlockRequestAsync(string containerName, string blobName, byte[] fullData, ArrayRangeWithBlockIdString range, string leaseId = null)
        {
            var md5Task = CalculateMD5Async(fullData, range.Offset, range.Length);

            var chunk = new byte[range.Length];
            Buffer.BlockCopy(fullData, range.Offset, chunk, 0, range.Length);

            return await PutBlockAsync(containerName, blobName, range.Id, chunk, await md5Task, leaseId);
        }

        private static string GenerateBlockId()
        {
            return Base64Converter.ConvertToBase64(Guid.NewGuid().ToString());
        }

        private async static Task<string> CalculateMD5Async(byte[] fullData, int offset, int length)
        {
            return await Task.Run(() => Convert.ToBase64String(MD5.Create().ComputeHash(fullData, offset, length)));
        }

        private static T RunSynchronously<T>(Func<Task<T>> function)
        {
            var smartTask = Task.Run(function);

            // GetAwaiter().GetResult() doesn't wrap the underlying exception in an AggregateException
            // http://blogs.msdn.com/b/pfxteam/archive/2011/09/28/task-exception-handling-in-net-4-5.aspx
            return smartTask.GetAwaiter().GetResult();
        }

        private struct ArrayRangeWithBlockIdString
        {
            public string Id { get; set; }
            public int Offset { get; set; }
            public int Length { get; set; }
        }
    }
}
