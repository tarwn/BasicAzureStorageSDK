using System;

namespace Basic.Azure.Storage.Communications.Core.Interfaces
{
    public interface IBlobPropertiesResponse
    {
        string ETag { get; }
        DateTime LastModified { get; }
        DateTime Date { get; }
        string ContentMD5 { get; }
    }
}