using System;

namespace Basic.Azure.Storage.Extensions.Contracts
{
    public interface IBlobOrBlockListResponseWrapper
    {
        string ETag { get; }

        DateTime LastModified { get; }

        DateTime Date { get; }

        string ContentMD5 { get; }
    }
}
