using Basic.Azure.Storage.Communications.Core.Interfaces;

namespace Basic.Azure.Storage.Extensions.Contracts
{
    public interface IBlobOrBlockListResponseWrapper : IBlobPropertiesResponse
    {
        IBlobPropertiesResponse Response { get; }
        bool IsPutBlobResponse { get; }
        bool IsPutBlockListResponse { get; }
    }
}