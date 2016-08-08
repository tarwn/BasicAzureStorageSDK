using Basic.Azure.Storage.ClientContracts;
using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.Common;
using Basic.Azure.Storage.Communications.ServiceExceptions;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Basic.Azure.Storage.Communications.Utility;
using Microsoft.WindowsAzure.Storage.Blob;
using BlobType = Microsoft.WindowsAzure.Storage.Blob.BlobType;
using LeaseDuration = Basic.Azure.Storage.Communications.Common.LeaseDuration;
using LeaseState = Basic.Azure.Storage.Communications.Common.LeaseState;
using LeaseStatus = Basic.Azure.Storage.Communications.Common.LeaseStatus;
using System.Configuration;

namespace Basic.Azure.Storage.Tests.Integration.BlobServiceClientTests
{
    [TestFixture]
    public class AccountOperationsTests
    {
        protected const string InvalidLeaseId = "InvalidLeaseId";

        private readonly string _azureConnectionString = ConfigurationManager.AppSettings["AzureConnectionString"];
        private readonly BlobUtil _util = new BlobUtil(ConfigurationManager.AppSettings["AzureConnectionString"]);
        private readonly string _runId = DateTime.Now.ToString("yyyy-MM-dd");

        protected StorageAccountSettings AccountSettings
        {
            get
            {
                return StorageAccountSettings.Parse(_azureConnectionString);
            }
        }
        
        protected static string FakeLeaseId { get { return "a28cf439-8776-4653-9ce8-4e3df49b4a72"; } }

        protected static DateTime GetTruncatedUtcNow()
        {
            return new DateTime(DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day, DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, DateTime.UtcNow.Second, DateTimeKind.Utc);
        }

        protected static string GetGuidString()
        {
            return Guid.NewGuid().ToString();
        }


        [TestFixtureTearDown]
        public void TestFixtureTeardown()
        {
            //let's clean up!
            _util.Cleanup();
        }

        #region Account Operations

        #endregion


    }
}
