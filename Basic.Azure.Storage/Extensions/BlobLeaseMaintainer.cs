using System;
using System.Threading;
using System.Threading.Tasks;
using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.BlobService.BlobOperations;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using Basic.Azure.Storage.Extensions.Contracts;

namespace Basic.Azure.Storage.Extensions
{
    public class BlobLeaseMaintainer : IDisposable
    {
        private readonly IBlobServiceClientEx _blobServiceClientEx;

        private bool _disposing = false;
        
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

        [Obsolete("Use LeaseNewOrExistingBlockBlobAsync instead. This method was misnamed and will be removed in a future version")]
        public static async Task<LeaseBlobAcquireResponse> LeaseNewOrExistingBlockBlob(IBlobServiceClientEx blobServiceClientEx, string containerName, string blobName, int leaseDuration, string proposedLeaseId = null)
        {
            return await LeaseNewOrExistingBlockBlobAsync(blobServiceClientEx, containerName, blobName, leaseDuration, proposedLeaseId);
        }

        public static async Task<LeaseBlobAcquireResponse> LeaseNewOrExistingBlockBlobAsync(IBlobServiceClientEx blobServiceClientEx, string containerName, string blobName, int leaseDuration, string proposedLeaseId = null)
        {
            try
            {
                return await LeaseExistingBlockBlobAsync(blobServiceClientEx, containerName, blobName, leaseDuration, proposedLeaseId);
            }
            catch (BlobNotFoundAzureException)
            {
                // ignore so that the blob can be created and leased below
                // if we could await in here, we'd just do it in here
            }
            await blobServiceClientEx.PutBlockBlobAsync(containerName, blobName, new byte[] { });
            return await LeaseExistingBlockBlobAsync(blobServiceClientEx, containerName, blobName, leaseDuration, proposedLeaseId);
        }

        [Obsolete("Use LeaseNewOrExistingBlockBlobAndMaintainLeaseAsync instead. This method was misnamed and will be removed in a future version")]
        public static async Task<BlobLeaseMaintainer> LeaseNewOrExistingBlockBlobAndMaintainLease(IBlobServiceClientEx blobServiceClientEx, string containerName, string blobName, int leaseDuration, string proposedLeaseId = null, Action<BlobLeaseMaintainer, AggregateException> exceptionHandler = null)
        {
            return await LeaseNewOrExistingBlockBlobAndMaintainLeaseAsync(blobServiceClientEx, containerName, blobName, leaseDuration, proposedLeaseId, exceptionHandler);
        }

        public static async Task<BlobLeaseMaintainer> LeaseNewOrExistingBlockBlobAndMaintainLeaseAsync(IBlobServiceClientEx blobServiceClientEx, string containerName, string blobName, int leaseDuration, string proposedLeaseId = null, Action<BlobLeaseMaintainer, AggregateException> exceptionHandler = null)
        {
            var leaseResponse = await LeaseNewOrExistingBlockBlobAsync(blobServiceClientEx, containerName, blobName, leaseDuration, proposedLeaseId);

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
                // Are there any cases where we would want to try to release the lease at this point?

                if (null != TaskExceptionHandler)
                {
                    TaskExceptionHandler(this, task.Exception);
                }
            }, TaskContinuationOptions.OnlyOnFaulted);
        }

        [Obsolete("Use StopMaintainingAndClearLeaseAsync instead. This method was misnamed and will be removed in a future version")]
        public async Task StopMaintainingAndClearLease()
        {
            await StopMaintainingAndClearLeaseAsync();
        }

        public async Task StopMaintainingAndClearLeaseAsync()
        {
            CancellationTokenSource.Cancel(true);
            try
            {
                await MaintainLeaseTask; // let the renewal task finish
            }
            catch (TaskCanceledException)
            { 
                // eat this exception - it seems we can get this while the task status doesn't reflect cancelled
            }
            await _blobServiceClientEx.LeaseBlobReleaseAsync(ContainerName, BlobName, LeaseId);
            TaskExceptionHandler = null;
        }

        private static async Task<LeaseBlobAcquireResponse> LeaseExistingBlockBlobAsync(IBlobServiceClient blobServiceClient, string containerName, string blobName, int leaseDuration, string proposedLeaseId)
        {
            return await blobServiceClient.LeaseBlobAcquireAsync(containerName, blobName, leaseDuration, proposedLeaseId ?? Guid.NewGuid().ToString());
        }

        public void Dispose()
        {
            if (!_disposing)
            {
                _disposing = true;
                StopMaintainingAndClearLeaseAsync().Wait(1000); // wait up to 1 second for the lease disposal to occur - arbitrary but reasonable amount of time
            }
        }
    }
}