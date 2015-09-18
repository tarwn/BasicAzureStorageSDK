using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Extensions;
using NUnit.Framework;

namespace Basic.Azure.Storage.Tests.Communications.BlobService
{
    [TestFixture]
    public class BlobServiceClientExTests
    {
        private readonly StorageAccountSettings _accountSettings = new LocalEmulatorAccountSettings();

        [Test]
        public void BlobServiceClientEx_DefaultParams_HasDefaultPutSingleBlobSizeEqualToConstant()
        {
            var client = new BlobServiceClientEx(_accountSettings);

            Assert.AreEqual(BlobServiceConstants.MaxSingleBlobUploadSize, client.MaxSingleBlobUploadSize);
        }

        [Test]
        public void BlobServiceClientEx_PutSingleBlobSize42Specified_HasPutSingleBlobSizeEqualTo42()
        {
            const int putSingleBlobSizeOverride = 42;
            var client = new BlobServiceClientEx(_accountSettings, putSingleBlobSizeOverride);

            Assert.AreEqual(putSingleBlobSizeOverride, client.MaxSingleBlobUploadSize);
        }

    }
}