using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Basic.Azure.Storage
{
    public class StorageAccountSettings
    {
        private const string ADDRESS_BASE_BLOB = "blob.core.windows.net";
        private const string ADDRESS_BASE_QUEUE = "queue.core.windows.net";
        private const string ADDRESS_BASE_TABLE = "table.core.windows.net";

        public StorageAccountSettings(string accountName, string accountKey, bool useHttps)
        {
            AccountName = accountName;
            AccountKey = accountKey;
            UseHttps = useHttps;
        }
        public string AccountName { get; protected set; }
        public string AccountKey { get; protected set; }
        public bool UseHttps { get; protected set; }

        public byte[] AccountKeyBytes { get { return Convert.FromBase64String(AccountKey); } }

        public virtual string BlobEndpoint { get { return GetEndpoint(ADDRESS_BASE_BLOB); } }
        public virtual string QueueEndpoint { get { return GetEndpoint(ADDRESS_BASE_QUEUE); } }
        public virtual string TableEndpoint { get { return GetEndpoint(ADDRESS_BASE_TABLE); } }
        
        private string GetEndpoint(string serviceAddressBase)
        {
            return String.Format("{0}://{1}.{2}", 
                UseHttps ? "https" : "http",
                AccountName,
                serviceAddressBase);
        }

        /// <summary>
        /// Computes the mac sha256 of the storage key
        /// </summary>
        public string ComputeMacSha256(string canonicalizedString)
        {
            byte[] dataToMAC = Encoding.UTF8.GetBytes(canonicalizedString);

            using (HMACSHA256 hmacsha1 = new HMACSHA256(AccountKeyBytes))
            {
                return System.Convert.ToBase64String(hmacsha1.ComputeHash(dataToMAC));
            }
        }

    }

    public class LocalEmulatorAccountSettings : StorageAccountSettings
    {
        protected const string DEV_DEFAULT_URL = "127.0.0.1";
        protected const string DEV_ACCOUNT_NAME = "devstoreaccount1";
        protected const string DEV_ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

        public LocalEmulatorAccountSettings(string proxyUri = DEV_DEFAULT_URL)
            : base(DEV_ACCOUNT_NAME, DEV_ACCOUNT_KEY, false)
        {
            ProxyUri = "ipv4.fiddler";// proxyUri;
        }

        public string ProxyUri { get; protected set; }

        public override string BlobEndpoint { get { return String.Format("http://{0}:10000/devstoreaccount1", ProxyUri); } }
        public override string QueueEndpoint { get { return String.Format("http://{0}:10001/devstoreaccount1", ProxyUri); } }
        public override string TableEndpoint { get { return String.Format("http://{0}:10002/devstoreaccount1", ProxyUri); } }
    }
}
