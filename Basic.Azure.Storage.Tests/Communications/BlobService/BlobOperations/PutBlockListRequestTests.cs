using Basic.Azure.Storage.Tests.Fakes;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.BlobService.BlobOperations;
using Basic.Azure.Storage.Communications.Utility;
using TestableHttpWebResponse;
using TestableHttpWebResponse.ResponseSettings;

namespace Basic.Azure.Storage.Tests.Communications.BlobService.BlobOperations
{
    [TestFixture]
    public class PutBlockListRequestTests
    {
        private StorageAccountSettings _settings;

        [TestFixtureSetUp]
        public void SetupFixture()
        {
            _settings = new SettingsFake();
            WebRequest.RegisterPrefix("test", TestableWebRequestCreateFactory.GetFactory());
        }

        [Test]
        public void Execute_BlockListWithAllHeaders_AllHeadersSetProperly()
        {
            var expectedContainer = "test-container";
            var expectedBlob = "test-blob";
            var expectedUri = String.Format("{0}/{1}/{2}?comp=blocklist", _settings.BlobEndpoint, expectedContainer, expectedBlob);
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri))
                                            .EnqueueResponse(new HttpResponseSettings((HttpStatusCode)201, "Created", "", false, new Dictionary<string, string>(){
                                                {"ETag", "\"123\""},
                                                {"Date", DateTime.UtcNow.ToString() },
                                                {"Last-Modified", DateTime.UtcNow.ToString() },
                                                {"Content-MD5", "123-MD5" }
                                            }));
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var expectedData = new BlockListBlockIdList();
            var expectedContentMD5 = expectedData.AsXmlByteArrayWithMd5Hash().MD5Hash;

            var request = new PutBlockListRequest(_settings, expectedContainer, expectedBlob, expectedData,
                "cache/control", "content/type", "content/encoding", "content/language", "blobContent/md5");

            request.Execute();

            Assert.AreEqual(expectedContentMD5, expectedRawRequest.Headers["Content-MD5"]);
            Assert.AreEqual("cache/control", expectedRawRequest.Headers["x-ms-blob-cache-control"]);
            Assert.AreEqual("content/type", expectedRawRequest.Headers["x-ms-blob-content-type"]);
            Assert.AreEqual("content/encoding", expectedRawRequest.Headers["x-ms-blob-content-encoding"]);
            Assert.AreEqual("content/language", expectedRawRequest.Headers["x-ms-blob-content-language"]);
            Assert.AreEqual("blobContent/md5", expectedRawRequest.Headers["x-ms-blob-content-md5"]);
        }

        [Test]
        public void Execute_BlockListWithContent_ContentSetProperly()
        {
            var expectedContainer = "test-container";
            var expectedBlob = "test-blob";
            var expectedUri = String.Format("{0}/{1}/{2}?comp=blocklist", _settings.BlobEndpoint, expectedContainer, expectedBlob);
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri))
                                            .EnqueueResponse(new HttpResponseSettings((HttpStatusCode)201, "Created", "", false, new Dictionary<string, string>(){
                                                {"ETag", "\"123\""},
                                                {"Date", DateTime.UtcNow.ToString() },
                                                {"Last-Modified", DateTime.UtcNow.ToString() },
                                                {"Content-MD5", "123-MD5" }
                                            }));
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var id1 = Base64Converter.ConvertToBase64("id1");
            var id2 = Base64Converter.ConvertToBase64("id2");
            var id3 = Base64Converter.ConvertToBase64("id3");
            var expectedData =
@"<?xml version=""1.0"" encoding=""utf-8""?><BlockList><Committed>" + id1 + @"</Committed><Uncommitted>" + id2 + @"</Uncommitted><Latest>" + id3 + @"</Latest></BlockList>";
            var givenData = new BlockListBlockIdList
            {
                new BlockListBlockId { Id = id1, ListType = BlockListListType.Committed},
                new BlockListBlockId { Id = id2, ListType = BlockListListType.Uncommitted},
                new BlockListBlockId { Id = id3, ListType = BlockListListType.Latest}
            };

            var request = new PutBlockListRequest(_settings, expectedContainer, expectedBlob, givenData);

            request.Execute();

            Assert.AreEqual(expectedData, Encoding.UTF8.GetString(givenData.AsXMLByteArray()));
        }

        [Test]
        public void Execute_BlockList_ResponseParsesHeadersCorrectly()
        {
            var expectedContainer = "test-container";
            var expectedBlob = "test-blob";
            var expectedUri = String.Format("{0}/{1}/{2}?comp=blocklist", _settings.BlobEndpoint, expectedContainer, expectedBlob);
            var expectedDate = DateTime.UtcNow.AddDays(-2345);
            var expectedRawRequest = new TestableWebRequest(new Uri(expectedUri))
                                            .EnqueueResponse(new HttpResponseSettings((HttpStatusCode)201, "Created", "", false, new Dictionary<string, string>(){
                                                {"ETag", "\"123\""},
                                                {"Date", expectedDate.ToString() },
                                                {"Last-Modified", expectedDate.ToString() },
                                                {"Content-MD5", "123-MD5" }
                                            }));
            TestableWebRequestCreateFactory.GetFactory().AddRequest(expectedRawRequest);
            var expectedData = new BlockListBlockIdList();

            var request = new PutBlockListRequest(_settings, expectedContainer, expectedBlob, expectedData);

            var response = request.Execute();

            Assert.AreEqual("123", response.Payload.ETag);
            Assert.IsTrue(Math.Abs(expectedDate.Subtract(response.Payload.Date).TotalMinutes) < 1);
            Assert.IsTrue(Math.Abs(expectedDate.Subtract(response.Payload.LastModified).TotalMinutes) < 1);
            Assert.AreEqual("123-MD5", response.Payload.ContentMD5);
        }
    }
}
