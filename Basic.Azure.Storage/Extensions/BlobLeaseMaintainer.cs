using System;
using System.Threading;
using System.Threading.Tasks;
using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.BlobService.BlobOperations;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Basic.Azure.Storage.Extensions.Contracts;

namespace Basic.Azure.Storage.Extensions
{
    public class BlobLeaseMaintainer
    {
        private readonly IBlobServiceClientEx _blobServiceClientEx;

        private Task MaintainLeaseTask { get; set; }
        private CancellationTokenSource CancellationTokenSource { get; set; }
        private Action<BlobLeaseMaintainer, AggregateException> TaskExceptionHandler { get; set; }

        public string LeaseId { get; set; }
        public int LeaseDuration { get; set; }
        public DateTime LeaseAcquiredOn { get; set; }
        public string ContainerName { get; private set; }
        public string BlobName { get; private set; }

        private BlobLeaseMaintainer(IBlobServiceClientEx blobServiceClientEx, string containerName, string blobName, string leaseId, DateTime leaseAcquiredOn, int leaseDuration, Action<BlobLeaseMaintainer, AggregateException> exceptionHandler)
        {
            _blobServiceClientEx = blobServiceClientEx;

            ContainerName = containerName;
            BlobName = blobName;
            LeaseId = leaseId;
            LeaseAcquiredOn = leaseAcquiredOn;
            LeaseDuration = leaseDuration;
            TaskExceptionHandler = exceptionHandler;

            CancellationTokenSource = new CancellationTokenSource();

            StartMaintainingLease();
        }

        public static async Task<LeaseBlobAcquireResponse> LeaseNewOrExistingBlockBlob(IBlobServiceClientEx blobServiceClientEx, string containerName, string blobName, int leaseDuration, string proposedLeaseId = null)
        {
            try
            {
                return await LeaseExistingBlockBlob(blobServiceClientEx, containerName, blobName, leaseDuration, proposedLeaseId);
            }
            catch (BlobNotFoundAzureException)
            {
                // ignore so that the blob can be created and leased below
                // if we could await in here, we'd just do it in here
            }
            await blobServiceClientEx.PutBlockBlobAsync(containerName, blobName, new byte[] { });
            return await LeaseExistingBlockBlob(blobServiceClientEx, containerName, blobName, leaseDuration, proposedLeaseId);
        }
        public static async Task<BlobLeaseMaintainer> LeaseNewOrExistingBlockBlobAndMaintainLease(IBlobServiceClientEx blobServiceClientEx, string containerName, string blobName, int leaseDuration, string proposedLeaseId = null, Action<BlobLeaseMaintainer, AggregateException> exceptionHandler = null)
        {
            var leaseResponse = await LeaseNewOrExistingBlockBlob(blobServiceClientEx, containerName, blobName, leaseDuration, proposedLeaseId);

            return new BlobLeaseMaintainer(blobServiceClientEx, containerName, blobName, leaseResponse.LeaseId, leaseResponse.Date, leaseDuration, exceptionHandler);
        }

        private void StartMaintainingLease()
        {
            var token = CancellationTokenSource.Token;

            MaintainLeaseTask = Task.Run(async () =>
            {
                var halfTime = LeaseDuration * 1000 / 2; // convert seconds to ms, then half

                while (true)
                {
                    token.WaitHandle.WaitOne(halfTime);

                    if (token.IsCancellationRequested)
                    {
                        break;
                    }

                    await _blobServiceClientEx.LeaseBlobRenewAsync(ContainerName, BlobName, LeaseId);
                }
            }, token);

            MaintainLeaseTask.ContinueWith(task =>
            {
                if (null != TaskExceptionHandler)
                {
                    TaskExceptionHandler(this, task.Exception);
                }
            }, TaskContinuationOptions.OnlyOnFaulted);
        }
        public async Task StopMaintainingAndClearLease()
        {
            CancellationTokenSource.Cancel(true);
            await MaintainLeaseTask; // let the renewal task finish
            await _blobServiceClientEx.LeaseBlobReleaseAsync(ContainerName, BlobName, LeaseId);
        }

        private static async Task<LeaseBlobAcquireResponse> LeaseExistingBlockBlob(IBlobServiceClient blobServiceClient, string containerName, string blobName, int leaseDuration, string proposedLeaseId)
        {
            return await blobServiceClient.LeaseBlobAcquireAsync(containerName, blobName, leaseDuration, proposedLeaseId ?? Guid.NewGuid().ToString());
        }
    }
}