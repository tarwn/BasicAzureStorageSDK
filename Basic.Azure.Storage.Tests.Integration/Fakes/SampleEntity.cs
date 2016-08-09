using Basic.Azure.Storage.Communications.TableService.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Basic.Azure.Storage.Tests.Integration.Fakes
{
    public class SampleEntity : ITableEntity
    {
        public string PartitionKey { get;set; }

        public string RowKey{ get;set;}

        public string ExtraValue { get; set; }
    }

    public class SampleMSEntity : Microsoft.WindowsAzure.Storage.Table.TableEntity
    {
        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public string ExtraValue { get; set; }
    }
}
