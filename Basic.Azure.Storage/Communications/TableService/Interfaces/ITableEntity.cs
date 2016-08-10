using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.TableService.Interfaces
{
    public interface ITableEntity
    {
        string PartitionKey { get; set; }
        string RowKey { get; set; }
    }
}
