using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.BlobService.BlobOperations;
using Basic.Azure.Storage.Communications.Utility;
using Basic.Azure.Storage.Extensions.Contracts;

namespace Basic.Azure.Storage.Extensions
{
    public class BlobServiceClientEx : BlobServiceClient, IBlobServiceClientEx
    {

        public BlobServiceClientEx(StorageAccountSettings account)
            : base(account)
        {
            
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
            string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null)
        {
            return (data.Length <= BlobServiceConstants.MaxSingleBlobUploadSize)
                ? new BlobOrBlockListResponseWrapper(await PutBlockBlobAsync(containerName, blobName, data, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata))
                : new BlobOrBlockListResponseWrapper(await PutBlockBlobAsListAsync(blockSize, containerName, blobName, data, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata));
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
            string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null)
        {
            var chunkIndex = 0;
            var concurrentBlockListBlockIdList = new ConcurrentBag<IndexedBlockId>();

            var putBlockRequests = GetArrayRanges(data.Length, blockSize)
                .Select(async range => 
                    await GeneratePutBlockRequestAsync(this, containerName, blobName, chunkIndex++, concurrentBlockListBlockIdList, data, range));
            await Task.WhenAll(putBlockRequests);

            var sortedBlockIds = concurrentBlockListBlockIdList
                .OrderBy(indexed => indexed.Index)
                .Select(indexed => indexed.BlockId);
            var actualBlockListBlockIdList = new BlockListBlockIdList(sortedBlockIds);

            return await PutBlockListAsync(containerName, blobName, actualBlockListBlockIdList,
                    cacheControl, contentType, contentEncoding, contentLanguage, contentMD5, metadata);
        }

        private static IEnumerable<ArrayRange> GetArrayRanges(int arrayLength, int rangeSize)
        {
            var remainder = arrayLength % rangeSize;
            var remainderPoint = arrayLength - remainder - 1;

            for (var currentIndex = 0; currentIndex < arrayLength; currentIndex += rangeSize)
            {
                yield return new ArrayRange
                {
                    Length = (currentIndex <= remainderPoint ? rangeSize : remainder),
                    Offset = currentIndex
                };
            }
        }

        private static async Task<byte[]> GenerateChunkFromRangeAsync(byte[] fullArray, ArrayRange range)
        {
            return await Task.Run(() =>
            {
                var chunk = new byte[range.Length];
                Array.Copy(fullArray, range.Offset, chunk, 0, range.Length);
                return chunk;
            });
        }

        private static async Task<PutBlockResponse> GeneratePutBlockRequestAsync(IBlobServiceClient blobServiceClient, string containerName, string blobName, int chunkIndex, ConcurrentBag<IndexedBlockId> blockIdBag, byte[] fullData, ArrayRange range)
        {
            var chunkTask = GenerateChunkFromRangeAsync(fullData, range);
            
            var currentBlockId = await GenerateBlockIdAsync();
            blockIdBag.Add(new IndexedBlockId
                {
                    BlockId = new BlockListBlockId { Id = currentBlockId, ListType = BlockListListType.Uncommitted },
                    Index = chunkIndex
                });

            var chunk = await chunkTask;
            return await blobServiceClient.PutBlockAsync(containerName, blobName, currentBlockId, chunk, await CalculateMD5Async(chunk));
        }

        private async static Task<string> GenerateBlockIdAsync()
        {
            return await Task.Run(() => Base64Converter.ConvertToBase64(Guid.NewGuid().ToString()));
        }

        private async static Task<string> CalculateMD5Async(byte[] data)
        {
            return await Task.Run(() => Convert.ToBase64String(MD5.Create().ComputeHash(data)));
        }

        private struct ArrayRange
        {
            public int Offset { get; set; }
            public int Length { get; set; }
        }

        private struct IndexedBlockId
        {
            public BlockListBlockId BlockId { get; set; }
            public int Index { get; set; }
        }
    }
}
