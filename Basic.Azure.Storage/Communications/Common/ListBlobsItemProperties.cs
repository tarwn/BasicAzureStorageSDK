using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Basic.Azure.Storage.Communications.Common
{
    public class ListBlobsItemProperties
    {

        public DateTime LastModified { get; set; }

        public string ETag { get; set; }

        public long ContentLength { get; set; }

        public string ContentType { get; set; }

        public string ContentEncoding { get; set; }

        public string ContentLanguage { get; set; }

        public string ContentMD5 { get; set; }

        public string CacheControl { get; set; }

        public string BlobSequenceNumber { get; set; }

        public BlobType BlobType { get; set; }

        public LeaseStatus LeaseStatus { get; set; }

        public LeaseState LeaseState { get; set;}

        public LeaseDuration LeaseDuration { get; set; }

        public string CopyId { get; set; }

        public CopyStatus? CopyStatus { get; set; }

        public Uri CopySource { get; set; }

        public string CopyProgress { get; set; }

        public DateTime? CopyCompletionTime { get; set; }

        public string CopyStatusDescription { get; set; }

    }
}
