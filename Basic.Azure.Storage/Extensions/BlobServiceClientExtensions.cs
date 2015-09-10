﻿using System;
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
            var rangesAndBlockIds = GetBlockRangesAndIds(data.Length, blockSize);

            var putBlockRequests = rangesAndBlockIds
                .Select(async blockInfo => await GeneratePutBlockRequestAsync(containerName, blobName, data, blockInfo))
                .ToList();

            var convertedBlockListBlockIds = rangesAndBlockIds
                .Select(blockInfo => new BlockListBlockId { Id = blockInfo.Id, ListType = BlockListListType.Uncommitted });
            var actualBlockIdList = new BlockListBlockIdList(convertedBlockListBlockIds);

            await Task.WhenAll(putBlockRequests);
            return await PutBlockListAsync(containerName, blobName, actualBlockIdList,
                    cacheControl, contentType, contentEncoding, contentLanguage, contentMD5, metadata);
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

        private async Task<PutBlockResponse> GeneratePutBlockRequestAsync(string containerName, string blobName, byte[] fullData, ArrayRangeWithBlockIdString range)
        {
            var md5Task = CalculateMD5(fullData, range.Offset, range.Length);

            var chunk = new byte[range.Length];
            Buffer.BlockCopy(fullData, range.Offset, chunk, 0, range.Length);

            return await PutBlockAsync(containerName, blobName, range.Id, chunk, await md5Task);
        }

        private static string GenerateBlockId()
        {
            return Base64Converter.ConvertToBase64(Guid.NewGuid().ToString());
        }

        private async static Task<string> CalculateMD5(byte[] fullData, int offset, int length)
        {
            return await Task.Run(() => Convert.ToBase64String(MD5.Create().ComputeHash(fullData, offset, length)));
        }

        private struct ArrayRangeWithBlockIdString
        {
            public string Id { get; set; }
            public int Offset { get; set; }
            public int Length { get; set; }
        }
    }
}
