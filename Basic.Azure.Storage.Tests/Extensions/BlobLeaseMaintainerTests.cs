using Basic.Azure.Storage.Communications.BlobService.BlobOperations;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Basic.Azure.Storage.Extensions;
using Basic.Azure.Storage.Extensions.Contracts;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Tests.Extensions
{
    [TestFixture]
    public class BlobLeaseMaintainerTests
    {

        [Test]
        public async Task LeaseNewOrExistingBlockBlobAsync_ExistingBlob_AcquiresLease()
        {
            var expectedLeaseResponse = new FakeLeaseBlobAcquireResponse("unit-test-abc123");
            var mockClientExtensions = new Mock<IBlobServiceClientEx>();
            // LeaseBlobAcquire - returns response on first try
            mockClientExtensions.Setup(ce => ce.LeaseBlobAcquireAsync("container", "blob", It.IsAny<int>(), It.IsAny<string>()))
                                .ReturnsAsync(expectedLeaseResponse);

            var leaseResult = await BlobLeaseMaintainer.LeaseNewOrExistingBlockBlobAsync(mockClientExtensions.Object, "container", "blob", 30);

            Assert.AreEqual(expectedLeaseResponse, leaseResult);
        }

        [Test]
        public async Task LeaseNewOrExistingBlockBlobAsync_NonExistentBlob_CreatesBlobAndAcquiresLease()
        {
            var expectedException = new BlobNotFoundAzureException("1",System.Net.HttpStatusCode.NotFound, "status", new Dictionary<string,string>(), null);
            var expectedLeaseResponse = new FakeLeaseBlobAcquireResponse("unit-test-abc123");
            var expectedBlobResponse = new PutBlobResponse();
            var mockClientExtensions = new Mock<IBlobServiceClientEx>();
            // LeaseBlobAcquire - blob not found on first call, good response on second
            mockClientExtensions.SetupSequence(ce => ce.LeaseBlobAcquireAsync("container", "blob", It.IsAny<int>(), It.IsAny<string>()))
                                .Throws(expectedException)
                                .Returns(Task.Run(() => (LeaseBlobAcquireResponse)expectedLeaseResponse));
            // PutBlockBlob
            mockClientExtensions.Setup(ce => ce.PutBlockBlobAsync("container", "blob", It.IsAny<byte[]>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<string>()))
                                .ReturnsAsync(expectedBlobResponse);

            var leaseResult = await BlobLeaseMaintainer.LeaseNewOrExistingBlockBlobAsync(mockClientExtensions.Object, "container", "blob", 30);

            Assert.AreEqual(expectedLeaseResponse, leaseResult);
            // 2 attempts to lease
            mockClientExtensions.Verify(ce => ce.LeaseBlobAcquireAsync("container", "blob", It.IsAny<int>(), It.IsAny<string>()), Times.Exactly(2));
            // 1 attempt to create blob
            mockClientExtensions.Verify(ce => ce.PutBlockBlobAsync("container", "blob", It.IsAny<byte[]>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<string>()), Times.Once());
        }

        [Test]
        public async Task LeaseNewOrExistingBlockBlobAndMaintainLeaseAsync_ExistingBlob_AcquiresLease()
        {
            var expectedLeaseResponse = new FakeLeaseBlobAcquireResponse("unit-test-abc123");
            var mockClientExtensions = new Mock<IBlobServiceClientEx>();
            // LeaseBlobAcquire - returns response on first try
            mockClientExtensions.Setup(ce => ce.LeaseBlobAcquireAsync("container", "blob", It.IsAny<int>(), It.IsAny<string>()))
                                .ReturnsAsync(expectedLeaseResponse);

            var leaseResult = await BlobLeaseMaintainer.LeaseNewOrExistingBlockBlobAndMaintainLeaseAsync(mockClientExtensions.Object, "container", "blob", 1);

            Assert.AreEqual(expectedLeaseResponse.LeaseId, leaseResult.LeaseId);
            // temporary until we fix lack of auto-release
            await leaseResult.StopMaintainingAndClearLeaseAsync();
        }

        [Test]
        public async void LeaseNewOrExistingBlockBlobAndMaintainLeaseAsync_NonExistentBlob_CreatesBlobAndAcquiresLease()
        {
            var expectedException = new BlobNotFoundAzureException("1", System.Net.HttpStatusCode.NotFound, "status", new Dictionary<string, string>(), null);
            var expectedLeaseResponse = new FakeLeaseBlobAcquireResponse("unit-test-abc123");
            var expectedBlobResponse = new PutBlobResponse();
            var mockClientExtensions = new Mock<IBlobServiceClientEx>();
            // LeaseBlobAcquire - blob not found on first call, good response on second
            mockClientExtensions.SetupSequence(ce => ce.LeaseBlobAcquireAsync("container", "blob", It.IsAny<int>(), It.IsAny<string>()))
                                .Throws(expectedException)
                                .Returns(Task.Run(() => (LeaseBlobAcquireResponse)expectedLeaseResponse));
            // PutBlockBlob
            mockClientExtensions.Setup(ce => ce.PutBlockBlobAsync("container", "blob", It.IsAny<byte[]>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<string>()))
                                .ReturnsAsync(expectedBlobResponse);

            var leaseResult = await BlobLeaseMaintainer.LeaseNewOrExistingBlockBlobAndMaintainLeaseAsync(mockClientExtensions.Object, "container", "blob", 2);

            Assert.AreEqual(expectedLeaseResponse.LeaseId, leaseResult.LeaseId);
            // temporary until we fix lack of auto-release
            await leaseResult.StopMaintainingAndClearLeaseAsync();
        }

        [Test]
        public async Task StopMaintainingAndClearLeaseAsync_ExistingLease_ReleasesLease()
        {
            var expectedLeaseResponse = new FakeLeaseBlobAcquireResponse("unit-test-abc123");
            var mockClientExtensions = new Mock<IBlobServiceClientEx>();
            // LeaseBlobAcquire - returns response on first try
            mockClientExtensions.Setup(ce => ce.LeaseBlobAcquireAsync("container", "blob", It.IsAny<int>(), It.IsAny<string>()))
                                .ReturnsAsync(expectedLeaseResponse);
            var lease = await BlobLeaseMaintainer.LeaseNewOrExistingBlockBlobAndMaintainLeaseAsync(mockClientExtensions.Object, "container", "blob", 30);

            await lease.StopMaintainingAndClearLeaseAsync();

            mockClientExtensions.Verify(ce => ce.LeaseBlobReleaseAsync("container", "blob", expectedLeaseResponse.LeaseId), Times.Once());
        }


        [Test]
        public async Task Dispose_ExistingLease_ReleasesLease()
        {
            var expectedLeaseResponse = new FakeLeaseBlobAcquireResponse("unit-test-abc123");
            var mockClientExtensions = new Mock<IBlobServiceClientEx>();
            // LeaseBlobAcquire - returns response on first try
            mockClientExtensions.Setup(ce => ce.LeaseBlobAcquireAsync("container", "blob", It.IsAny<int>(), It.IsAny<string>()))
                                .ReturnsAsync(expectedLeaseResponse);
            var lease = await BlobLeaseMaintainer.LeaseNewOrExistingBlockBlobAndMaintainLeaseAsync(mockClientExtensions.Object, "container", "blob", 30);

            lease.Dispose();

            mockClientExtensions.Verify(ce => ce.LeaseBlobReleaseAsync("container", "blob", expectedLeaseResponse.LeaseId), Times.Once());
        }

        /// <summary>
        /// This is exactly the same as the prior test, I just wanted to have an explicit example of the using in here
        /// </summary>
        [Test]
        public async Task Using_ExistingLease_ReleasesLease()
        {
            var expectedLeaseResponse = new FakeLeaseBlobAcquireResponse("unit-test-abc123");
            var mockClientExtensions = new Mock<IBlobServiceClientEx>();
            // LeaseBlobAcquire - returns response on first try
            mockClientExtensions.Setup(ce => ce.LeaseBlobAcquireAsync("container", "blob", It.IsAny<int>(), It.IsAny<string>()))
                                .ReturnsAsync(expectedLeaseResponse);
            
            using (var lease = await BlobLeaseMaintainer.LeaseNewOrExistingBlockBlobAndMaintainLeaseAsync(mockClientExtensions.Object, "container", "blob", 30)) { 
                // blah-di-blah, work going on here
            }

            mockClientExtensions.Verify(ce => ce.LeaseBlobReleaseAsync("container", "blob", expectedLeaseResponse.LeaseId), Times.Once());
        }

        [Test]
        public async Task Dispose_ExistingLeaseMultipleDisposes_ReleasesLeaseOnlyOnce()
        {
            var expectedLeaseResponse = new FakeLeaseBlobAcquireResponse("unit-test-abc123");
            var mockClientExtensions = new Mock<IBlobServiceClientEx>();
            // LeaseBlobAcquire - returns response on first try
            mockClientExtensions.Setup(ce => ce.LeaseBlobAcquireAsync("container", "blob", It.IsAny<int>(), It.IsAny<string>()))
                                .ReturnsAsync(expectedLeaseResponse);
            var lease = await BlobLeaseMaintainer.LeaseNewOrExistingBlockBlobAndMaintainLeaseAsync(mockClientExtensions.Object, "container", "blob", 30);

            lease.Dispose();

            mockClientExtensions.Verify(ce => ce.LeaseBlobReleaseAsync("container", "blob", expectedLeaseResponse.LeaseId), Times.Once());
        }


        [Test]
        public async Task LeaseNewOrExistingBlockBlobAndMaintainLeaseAsync_ExceptionDuringMaintenanceAndHandlerProvided_CallsExceptionHandler()
        {
            var expectedLeaseResponse = new FakeLeaseBlobAcquireResponse("unit-test-abc123");
            var expectedException = new Exception("Error, the internet is on fire");
            var mockClientExtensions = new Mock<IBlobServiceClientEx>();
            // LeaseBlobAcquire - returns response on first try
            mockClientExtensions.Setup(ce => ce.LeaseBlobAcquireAsync("container", "blob", It.IsAny<int>(), It.IsAny<string>()))
                                .ReturnsAsync(expectedLeaseResponse);
            // LeaseBlobRenewAsync - throws on the first attempted renewal
            mockClientExtensions.Setup(ce => ce.LeaseBlobRenewAsync("container", "blob", expectedLeaseResponse.LeaseId))
                                .Throws(expectedException);
            // wiring to wait until the exception and capture the passed exception
            AggregateException receivedException = null;
            var fakeExceptionHandlerTokenSource = new CancellationTokenSource();
            Action<BlobLeaseMaintainer, AggregateException> fakeExceptionHandler = (m, e) => {
                receivedException = e;
                fakeExceptionHandlerTokenSource.Cancel();
            };
            var lease = await BlobLeaseMaintainer.LeaseNewOrExistingBlockBlobAndMaintainLeaseAsync(mockClientExtensions.Object, "container", "blob", 1, null, fakeExceptionHandler);

            fakeExceptionHandlerTokenSource.Token.WaitHandle.WaitOne(2000);    // should cancel in less than one second

            Assert.AreEqual(expectedException, receivedException.InnerException);
        }

        [Test]
        public async Task LeaseNewOrExistingBlockBlobAndMaintainLeaseAsync_ExceptionDuringMaintenanceAndNoHandlerProvided_DoesntDoAnythingHorrible()
        {
            var expectedLeaseResponse = new FakeLeaseBlobAcquireResponse("unit-test-abc123");
            var expectedException = new Exception("Error, the internet is on fire");
            var mockClientExtensions = new Mock<IBlobServiceClientEx>();
            // LeaseBlobAcquire - returns response on first try
            mockClientExtensions.Setup(ce => ce.LeaseBlobAcquireAsync("container", "blob", It.IsAny<int>(), It.IsAny<string>()))
                                .ReturnsAsync(expectedLeaseResponse);
            // LeaseBlobRenewAsync - throws on the first attempted renewal
            var leaseRenewTokenSource = new CancellationTokenSource();
            mockClientExtensions.Setup(ce => ce.LeaseBlobRenewAsync("container", "blob", expectedLeaseResponse.LeaseId))
                                .Callback(() => {
                                    leaseRenewTokenSource.CancelAfter(100); // reasonable guess for far longer than it should take the maintenance task to die
                                })
                                .Throws(expectedException);
            var lease = await BlobLeaseMaintainer.LeaseNewOrExistingBlockBlobAndMaintainLeaseAsync(mockClientExtensions.Object, "container", "blob", 1, null, null);

            // ???

            // This test is highly questionable, there is no way to verify that we exited the maintenance
            var wasCancelled = leaseRenewTokenSource.Token.WaitHandle.WaitOne(2000);    // arbitrary amount much larger than lease time above
            Assert.IsTrue(wasCancelled);
        }
    }

    public class FakeLeaseBlobAcquireResponse : LeaseBlobAcquireResponse
    {
        public FakeLeaseBlobAcquireResponse(string leaseId) 
        {
            LeaseId = leaseId;
        }
    }
}
