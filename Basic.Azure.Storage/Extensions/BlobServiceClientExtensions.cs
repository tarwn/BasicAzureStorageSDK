using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
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
            return Task.Run(() =>
                PutBlockBlobIntelligentlyAsync(blockSize, containerName, blobName, data, leaseId, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata))
                .Result;
        }
        public IBlobOrBlockListResponseWrapper PutBlockBlobIntelligently(int blockSize,
            string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null)
        {
            return Task.Run(() =>
                PutBlockBlobIntelligentlyAsync(blockSize, containerName, blobName, data, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata))
                .Result;
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
            string containerName, string blobName, Stream data, string leaseId,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null)
        {
            Guard.ArgumentIsNotNull("data", data);
            if (data.CanSeek && data.Length < MaxSingleBlobUploadSize)
            {
                var dataBytes = new byte[data.Length];
                await data.ReadAsync(dataBytes, 0, (int)data.Length);
                return await PutBlockBlobAsync(containerName, blobName, dataBytes, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata, leaseId);
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
        public async Task<IBlobOrBlockListResponseWrapper> PutBlockBlobIntelligentlyAsync(int blockSize,
            string containerName, string blobName, Stream data,
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
            return Task.Run(() =>
                    PutBlockBlobAsListAsync(blockSize, containerName, blobName, data, leaseId, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata))
                    .Result;
        }
        public PutBlockListResponse PutBlockBlobAsList(int blockSize,
            string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null)
        {
            return Task.Run(() =>
                    PutBlockBlobAsListAsync(blockSize, containerName, blobName, data, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata))
                    .Result;
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
            string containerName, string blobName, Stream data, string leaseId,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null)
        {
            Guard.ArgumentIsNotNull("data", data);
            Guard.StreamIsReadable("data", data);

            var putBlockRequests = new List<Task<PutBlockResponse>>();
            var blockIdList = new BlockListBlockIdList();

            foreach (var blockData in PollStream(data, blockSize))
            {
                var blockId = GenerateBlockId();
                blockIdList.Add(new BlockListBlockId { Id = blockId, ListType = BlockListListType.Uncommitted });
                putBlockRequests.Add(GeneratePutBlockRequestAsync(containerName, blobName, blockData, blockId, leaseId));
            }

            await Task.WhenAll(putBlockRequests);
            return await PutBlockListAsync(containerName, blobName, blockIdList,
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
        public async Task<PutBlockListResponse> PutBlockBlobAsListAsync(int blockSize,
            string containerName, string blobName, Stream data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null)
        {
            var lease = await BlobLeaseMaintainer.LeaseNewOrExistingBlockBlobAndMaintainLease(this, containerName, blobName, 60 /* seconds */);

            var putResult = await PutBlockBlobAsListAsync(blockSize, containerName, blobName, data, lease.LeaseId, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata);

            await lease.StopMaintainingAndClearLease();

            return putResult;
        }

        private static IEnumerable<byte[]> PollStream(Stream stream, int requestedBlockSize)
        {
            int amountOfBytesRead;
            do
            {
                var block = new byte[requestedBlockSize];
                amountOfBytesRead = stream.Read(block, 0, requestedBlockSize);

                if (amountOfBytesRead > 0)
                {
                    yield return (amountOfBytesRead == requestedBlockSize ? block : SubArray(block, 0, amountOfBytesRead));
                }
            } while (amountOfBytesRead > 0);
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
                .Select(blockInfo => new BlockListBlockId { Id = blockInfo.Id, ListType = BlockListListType.Uncommitted });

            return new BlockListBlockIdList(convertedBlockListBlockIds);
        }

        private async Task<PutBlockResponse> GeneratePutBlockRequestAsync(string containerName, string blobName, byte[] fullData, ArrayRangeWithBlockIdString range, string leaseId = null)
        {
            var md5Task = CalculateMD5Async(fullData, range.Offset, range.Length);
            var chunk = SubArray(fullData, range.Offset, range.Length);

            return await PutBlockAsync(containerName, blobName, range.Id, chunk, await md5Task, leaseId);
        }
        private async Task<PutBlockResponse> GeneratePutBlockRequestAsync(string containerName, string blobName, byte[] data, string blockId, string leaseId = null)
        {
            var md5 = await CalculateMD5Async(data);

            return await PutBlockAsync(containerName, blobName, blockId, data, md5, leaseId);
        }

        private static string GenerateBlockId()
        {
            return Base64Converter.ConvertToBase64(Guid.NewGuid().ToString());
        }

        private async static Task<string> CalculateMD5Async(byte[] data)
        {
            return await Task.Run(() => Convert.ToBase64String(MD5.Create().ComputeHash(data)));
        }
        private async static Task<string> CalculateMD5Async(byte[] fullData, int offset, int length)
        {
            return await Task.Run(() => Convert.ToBase64String(MD5.Create().ComputeHash(fullData, offset, length)));
        }

        private static byte[] SubArray(byte[] fullArray, int offset, int length)
        {
            var subArray = new byte[length];
            Buffer.BlockCopy(fullArray, offset, subArray, 0, length);
            return subArray;
        }

        private struct ArrayRangeWithBlockIdString
        {
            public string Id { get; set; }
            public int Offset { get; set; }
            public int Length { get; set; }
        }
    }
}
