using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Tests.Fakes
{
    public class SettingsFake : StorageAccountSettings
    {
        public static string FakeKey = Convert.ToBase64String(Encoding.ASCII.GetBytes("unit-test"));

        public SettingsFake()
            : base("unit-test", FakeKey, false)
        { }

        public override string BlobEndpoint { get { return "test://blob.abc"; } }
        public override string QueueEndpoint { get { return "test://queue.abc"; } }
        public override string TableEndpoint { get { return "test://table.abc"; } }

    }
}
