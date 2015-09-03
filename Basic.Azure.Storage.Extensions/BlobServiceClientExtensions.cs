using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.BlobService.BlobOperations;
using Basic.Azure.Storage.Communications.Utility;

namespace Basic.Azure.Storage.Extensions
{
    public static class BlobServiceClientExtensions
    {
        private static readonly MD5 _md5 = MD5.Create();

        public static BlobOrBlockListResponseWrapper PutBlockBlobIntelligently(this IBlobServiceClient blobServiceClient, int blockSize,
            string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null)
        {
            return Task.Run(() => blobServiceClient.PutBlockBlobIntelligentlyAsync(blockSize, containerName, blobName, data, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata)).Result;
        }

        public async static Task<BlobOrBlockListResponseWrapper> PutBlockBlobIntelligentlyAsync(this IBlobServiceClient blobServiceClient, int blockSize,
            string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null)
        {


            if (data.Length <= BlobServiceConstants.MaxSingleBlobUploadSize)
            {
                return new BlobOrBlockListResponseWrapper(
                    await blobServiceClient.PutBlockBlobAsync(containerName, blobName, data, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata));
            }

            return new BlobOrBlockListResponseWrapper(
                await blobServiceClient.PutBlockBlobAsListAsync(blockSize, containerName, blobName, data, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata));
        }

        public static PutBlockListResponse PutBlockBlobAsList(this IBlobServiceClient blobServiceClient, int blockSize,
            string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null)
        {
            return Task.Run(() =>
                    blobServiceClient.PutBlockBlobAsListAsync(blockSize, containerName, blobName, data, contentType, contentEncoding, contentLanguage, contentMD5, cacheControl, metadata))
                    .Result;
        }

        public async static Task<PutBlockListResponse> PutBlockBlobAsListAsync(this IBlobServiceClient blobServiceClient, int blockSize,
            string containerName, string blobName, byte[] data,
            string contentType = null, string contentEncoding = null, string contentLanguage = null, string contentMD5 = null,
            string cacheControl = null, Dictionary<string, string> metadata = null)
        {
            var putBlockResponses = new List<Task<PutBlockResponse>>();
            var blockListBlockIdList = new BlockListBlockIdList();
            var remainder = data.Length % blockSize;
            var remainderPoint = data.Length - remainder - 1;

            for (var currentIndex = 0; currentIndex < data.Length; currentIndex += blockSize)
            {
                var currentBlockId = GenerateBlockId();

                var bufferLength = (currentIndex <= remainderPoint
                    ? blockSize
                    : remainder);
                var dataChunk = new byte[bufferLength];
                Array.Copy(data, currentIndex, dataChunk, 0, bufferLength);

                var dataChunkMD5 = CalculateMD5(dataChunk);

                var response = blobServiceClient
                    .PutBlockAsync(containerName, blobName, currentBlockId, dataChunk, dataChunkMD5);
                putBlockResponses.Add(response);

                blockListBlockIdList.Add(new BlockListBlockId { Id = currentBlockId, ListType = BlockListListType.Uncommitted });
            }

            await Task.WhenAll(putBlockResponses);

            return await blobServiceClient.PutBlockListAsync(containerName, blobName, blockListBlockIdList,
                cacheControl, contentType, contentEncoding, contentLanguage, contentMD5, metadata);
        }

        private static string GenerateBlockId()
        {
            return Base64Converter.ConvertToBase64(Guid.NewGuid().ToString());
        }

        private static string CalculateMD5(byte[] data)
        {
            return Convert.ToBase64String(_md5.ComputeHash(data));
        }
    }
}
