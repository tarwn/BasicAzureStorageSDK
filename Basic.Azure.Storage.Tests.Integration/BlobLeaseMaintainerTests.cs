using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Basic.Azure.Storage.Communications.BlobService.BlobOperations;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Basic.Azure.Storage.Extensions;
using Basic.Azure.Storage.Extensions.Contracts;
using Moq;
using NUnit.Framework;

namespace Basic.Azure.Storage.Tests.Integration
{
    [TestFixture]
    public class BlobLeaseMaintainerTests
    {

        [Test]
        public async void LeaseNewOrExistingBlockBlobAndMaintainLease_OnExceptionInMaintainTask_CallsProvidedExceptionHandler()
        {
            var exceptionHandlerCalled = false;
            Action<BlobLeaseMaintainer, AggregateException> exceptionHandler = (blm, ex) => exceptionHandlerCalled = true;
            var leaseAcquireResponseMock = new Mock<LeaseBlobAcquireResponse>();
            leaseAcquireResponseMock
                .Setup(_ => _.Date)
                .Returns(DateTime.Now);
            leaseAcquireResponseMock
                .Setup(_ => _.LeaseId)
                .Returns(Guid.NewGuid().ToString);
            var blobServiceClientExMock = new Mock<IBlobServiceClientEx>();
            blobServiceClientExMock
                .Setup(_ => _.LeaseBlobAcquireAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<string>()))
                .Returns(Task.FromResult(leaseAcquireResponseMock.Object));
            blobServiceClientExMock
                .Setup(_ => _.LeaseBlobRenewAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
                .Throws(new LeaseAlreadyBrokenAzureException("requestId", HttpStatusCode.SeeOther, "daquell", new Dictionary<string, string>(), new WebException()))
                .Verifiable();

            await BlobLeaseMaintainer.LeaseNewOrExistingBlockBlobAndMaintainLease(blobServiceClientExMock.Object, "testContainer", "testBlob", 15, null, exceptionHandler);

            Thread.Sleep(10000);
            blobServiceClientExMock.VerifyAll();
            Assert.True(exceptionHandlerCalled);
        }

        [Test]
        public async void LeaseNewOrExistingBlockBlobAndMaintainLease_OnExceptionInMaintainTask_DoesNothingIfNoHandlerProvided()
        {
            var leaseAcquireResponseMock = new Mock<LeaseBlobAcquireResponse>();
            leaseAcquireResponseMock
                .Setup(_ => _.Date)
                .Returns(DateTime.Now);
            leaseAcquireResponseMock
                .Setup(_ => _.LeaseId)
                .Returns(Guid.NewGuid().ToString);
            var blobServiceClientExMock = new Mock<IBlobServiceClientEx>();
            blobServiceClientExMock
                .Setup(_ => _.LeaseBlobAcquireAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<string>()))
                .Returns(Task.FromResult(leaseAcquireResponseMock.Object));
            blobServiceClientExMock
                .Setup(_ => _.LeaseBlobRenewAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
                .Throws(new LeaseAlreadyBrokenAzureException("requestId", HttpStatusCode.SeeOther, "daquell", new Dictionary<string, string>(), new WebException()))
                .Verifiable();

            await BlobLeaseMaintainer.LeaseNewOrExistingBlockBlobAndMaintainLease(blobServiceClientExMock.Object, "testContainer", "testBlob", 15);

            Thread.Sleep(10000);
            blobServiceClientExMock.VerifyAll();
        }

    }
}