using Basic.Azure.Storage.Communications.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Basic.Azure.Storage
{
    public class StorageAccountSettings
    {
        private const string ADDRESS_BASE_BLOB = "blob";
        private const string ADDRESS_BASE_QUEUE = "queue";
        private const string ADDRESS_BASE_TABLE = "table";
        private const string ADDRESS_DEFAULT_SUFFIX = "core.windows.net";

        protected const string DEV_ACCOUNT_NAME = "devstoreaccount1";
        protected const string DEV_ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
        protected const string DEV_DEFAULT_URL = "127.0.0.1";


        private readonly string _blobEndpoint;
        private readonly string _queueEndpoint;
        private readonly string _tableEndpoint;

        public StorageAccountSettings(string accountName, string accountKey, bool useHttps = true,
                                      string blobEndpoint = null, string queueEndpoint = null, string tableEndpoint = null,
                                      string endpointSuffix = null)
        {
            AccountName = accountName;
            AccountKey = accountKey;
            UseHttps = useHttps;

            var suffix = endpointSuffix ?? ADDRESS_DEFAULT_SUFFIX;
            _blobEndpoint = blobEndpoint ?? GetEndpoint(ADDRESS_BASE_BLOB, suffix);
            _queueEndpoint = queueEndpoint ?? GetEndpoint(ADDRESS_BASE_QUEUE, suffix);
            _tableEndpoint = tableEndpoint ?? GetEndpoint(ADDRESS_BASE_TABLE, suffix);
        }
        public string AccountName { get; protected set; }
        public string AccountKey { get; protected set; }
        public bool UseHttps { get; protected set; }

        public byte[] AccountKeyBytes { get { return Convert.FromBase64String(AccountKey); } }

        public virtual string BlobEndpoint { get { return _blobEndpoint; } }
        public virtual string QueueEndpoint { get { return _queueEndpoint; } }
        public virtual string TableEndpoint { get { return _tableEndpoint; } }

        private string GetEndpoint(string serviceAddressBase, string suffix)
        {
            return String.Format("{0}://{1}.{2}.{3}",
                UseHttps ? "https" : "http",
                AccountName,
                serviceAddressBase,
                suffix);
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


        public static StorageAccountSettings Parse(string connectionString)
        {
            var settings = connectionString.Split(';')
                                           .Select(s => s.Split(new char[] { '=' }, 2))
                                           .Where(s => s.Count() >= 1)
                                           .ToDictionary(s => s[0], s => s.Count() == 2 ? s[1] : "");

            if (IsDevelopmentStorage(settings))
            {
                string proxyUri = settings.ContainsKey("DevelopmentStorageProxyUri") ? settings["DevelopmentStorageProxyUri"] : null;
                return new LocalEmulatorAccountSettings(proxyUri);
            }

            var accountName = settings.ContainsKey("AccountName") ? settings["AccountName"] : null;
            var accountKey = settings.ContainsKey("AccountKey") ? settings["AccountKey"] : null;
            // only use the less secure method if we got exactly what we're looking for
            bool useHttp = settings.ContainsKey("DefaultEndpointsProtocol") && settings["DefaultEndpointsProtocol"].Equals("http", StringComparison.InvariantCultureIgnoreCase);

            Guard.ArgumentIsNotNullOrEmpty("AccountName", accountName);
            Guard.ArgumentIsNotNullOrEmpty("AccountKey", accountKey);


            var blobEndpoint = settings.ContainsKey("BlobEndpoint") ? settings["BlobEndpoint"] : null;
            var queueEndpoint = settings.ContainsKey("QueueEndpoint") ? settings["QueueEndpoint"] : null;
            var tableEndpoint = settings.ContainsKey("TableEndpoint") ? settings["TableEndpoint"] : null;

            var endpointSuffix = settings.ContainsKey("EndpointSuffix") ? settings["EndpointSuffix"] : null;

            return new StorageAccountSettings(accountName, accountKey, !useHttp, blobEndpoint, queueEndpoint, tableEndpoint, endpointSuffix);
        }

        private static bool IsDevelopmentStorage(Dictionary<string, string> settings)
        {
            if (settings.ContainsKey("UseDevelopmentStorage"))
            {
                if (settings["UseDevelopmentStorage"].Equals("true", StringComparison.InvariantCultureIgnoreCase))
                    return true;
                else
                    throw new ArgumentOutOfRangeException("UseDevelopmentStorage", "UseDevelopmentStorage should only be passed witha value of 'true' or not used at all.");
            }
            else if (settings.ContainsKey("AccountName") && settings["AccountName"].Equals(DEV_ACCOUNT_NAME, StringComparison.InvariantCultureIgnoreCase) &&
                     settings.ContainsKey("AccountKey") && settings["AccountKey"].Equals(DEV_ACCOUNT_KEY, StringComparison.InvariantCultureIgnoreCase))
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }

    public class LocalEmulatorAccountSettings : StorageAccountSettings
    {
        public LocalEmulatorAccountSettings(string proxyUri = null)
            : base(DEV_ACCOUNT_NAME, DEV_ACCOUNT_KEY, false)
        {
            ProxyUri = proxyUri ?? String.Format("http://{0}", DEV_DEFAULT_URL);
        }

        public string ProxyUri { get; protected set; }

        public override string BlobEndpoint { get { return String.Format("{0}:10000/devstoreaccount1", ProxyUri); } }
        public override string QueueEndpoint { get { return String.Format("{0}:10001/devstoreaccount1", ProxyUri); } }
        public override string TableEndpoint { get { return String.Format("{0}:10002/devstoreaccount1", ProxyUri); } }
    }
}
