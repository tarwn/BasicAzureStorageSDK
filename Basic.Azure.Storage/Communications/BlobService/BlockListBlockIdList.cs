using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace Basic.Azure.Storage.Communications.BlobService
{
    public class BlockListBlockIdList : List<BlockListBlockId>
    {
        public BlockListBlockIdList()
        {

        }

        public BlockListBlockIdList(int capacity)
            : base(capacity)
        {

        }

        public BlockListBlockIdList(IEnumerable<BlockListBlockId> collection)
            : base(collection)
        {

        }

        public XMLBytesWithMD5Hash AsXmlByteArrayWithMd5Hash()
        {
            var xmlBytes = AsXMLByteArray();
            var hash = Convert.ToBase64String(MD5.Create().ComputeHash(xmlBytes));

            return new XMLBytesWithMD5Hash()
            {
                MD5Hash = hash,
                XmlBytes = xmlBytes
            };
        }

        public byte[] AsXMLByteArray()
        {
            const string header = "<?xml version=\"1.0\" encoding=\"utf-8\"?>";
            const string blocklistOpening = "<BlockList>";
            const string blocklistClosing = "</BlockList>";
            const string blockFormat = "<{0}>{1}</{0}>";

            var writer = new StringBuilder(header);

            writer.Append(blocklistOpening);

            foreach (var curBlockListBlockId in this)
            {
                writer.AppendFormat(blockFormat, ElementNameFromBlockListBlockId(curBlockListBlockId), curBlockListBlockId.Id);
            }

            writer.Append(blocklistClosing);

            return Encoding.UTF8.GetBytes(writer.ToString());
        }

        private static string ElementNameFromBlockListBlockId(BlockListBlockId blockId)
        {
            string element;
            switch (blockId.ListType)
            {
                case BlockListListType.Committed:
                    element = "Committed";
                    break;
                case BlockListListType.Latest:
                    element = "Latest";
                    break;
                case BlockListListType.Uncommitted:
                    element = "Uncommitted";
                    break;
                default:
                    throw new ArgumentException("Given block list block id does not have a valid list type.", "blockId");
            }

            return element;
        }

        public struct XMLBytesWithMD5Hash
        {
            public byte[] XmlBytes { get; set; }
            public string MD5Hash { get; set; }
        }
    }
}
