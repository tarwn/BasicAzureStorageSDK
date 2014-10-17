using Basic.Azure.Storage.Communications.TableService;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.ClientContracts
{
    public interface ITableServiceClient
    {

        #region Account Operations

        #endregion

        #region Table Operations

        CreateTableResponse CreateTable(string tableName, MetadataPreference? metadataPreference = null);
        Task<CreateTableResponse> CreateTableAsync(string tableName, MetadataPreference? metadataPreference = null);

        #endregion

        #region Entity Operations

        #endregion
    }
}
