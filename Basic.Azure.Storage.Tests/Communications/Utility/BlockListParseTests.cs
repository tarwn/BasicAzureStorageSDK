using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;
using Basic.Azure.Storage.Communications.BlobService;
using Basic.Azure.Storage.Communications.Utility;
using NUnit.Framework;

namespace Basic.Azure.Storage.Tests.Communications.Utility
{
    [TestFixture]
    public class BlockListParseTests
    {
        [Test]
        public void ParseAllXmlBlockLists_WithCommittedAndUncommitted_ParsesCorrectly()
        {
            const string xmlDocument =
@"<?xml version=""1.0"" encoding=""utf-8""?>
<BlockList>
  <CommittedBlocks>
     <Block>
        <Name>YnJpYW4=</Name>
        <Size>6</Size>
     </Block>
     <Block>
        <Name>d2Fz</Name>
        <Size>20</Size>
     </Block>
  </CommittedBlocks>
  <UncommittedBlocks>
    <Block>
      <Name>aGVyZQ==</Name>
      <Size>1024</Size>
    </Block>
    <Block>
      <Name>c2luY2VyZWx5</Name>
      <Size>9</Size>
    </Block>
  </UncommittedBlocks>
 </BlockList>";
            var xDoc = XDocument.Parse(xmlDocument);
            var expectedBlockList = new Dictionary<GetBlockListListType, List<ParsedBlockListBlockId>>
            {
                {
                    GetBlockListListType.Committed, new List<ParsedBlockListBlockId>
                    {
                        new ParsedBlockListBlockId {Name = "YnJpYW4=", Size = 6},
                        new ParsedBlockListBlockId {Name = "d2Fz", Size = 20}
                    }
                },
                {
                    GetBlockListListType.Uncommitted, new List<ParsedBlockListBlockId>
                    {
                        new ParsedBlockListBlockId {Name = "aGVyZQ==", Size = 1024},
                        new ParsedBlockListBlockId {Name = "c2luY2VyZWx5", Size = 9}
                    }
                }
            };

            var parsedXml = Parsers.ParseAllXmlBlockLists(xDoc.Root);

            AssertBlockListDictionariesEqual(expectedBlockList, parsedXml);
        }
        
        [Test]
        public void ParseAllXmlBlockLists_WithCommitted_ParsesCorrectly()
        {
            const string xmlDocument =
                @"<?xml version=""1.0"" encoding=""utf-8""?>
<BlockList>
  <CommittedBlocks>
     <Block>
        <Name>YnJpYW4=</Name>
        <Size>6</Size>
     </Block>
     <Block>
        <Name>d2Fz</Name>
        <Size>20</Size>
     </Block>
  </CommittedBlocks>  
 </BlockList>";
            var xmlRoot = XDocument.Parse(xmlDocument);
            var expectedBlockList = new Dictionary<GetBlockListListType, List<ParsedBlockListBlockId>>
            {
                {
                    GetBlockListListType.Committed, new List<ParsedBlockListBlockId>
                    {
                        new ParsedBlockListBlockId {Name = "YnJpYW4=", Size = 6},
                        new ParsedBlockListBlockId {Name = "d2Fz", Size = 20}
                    }
                },
                {
                    GetBlockListListType.Uncommitted, new List<ParsedBlockListBlockId>()
                }
            };

            var parsedXml = Parsers.ParseAllXmlBlockLists(xmlRoot.Root);

            AssertBlockListDictionariesEqual(expectedBlockList, parsedXml);
        }

        [Test]
        public void ParseAllXmlBlockLists_WithUncommitted_ParsesCorrectly()
        {
            const string xmlDocument =
                @"<?xml version=""1.0"" encoding=""utf-8""?>
<BlockList>  
  <UncommittedBlocks>
    <Block>
      <Name>aGVyZQ==</Name>
      <Size>1024</Size>
    </Block>
    <Block>
      <Name>c2luY2VyZWx5</Name>
      <Size>9</Size>
    </Block>
  </UncommittedBlocks>
 </BlockList>";
            var xmlRoot = XDocument.Parse(xmlDocument);
            var expectedBlockList = new Dictionary<GetBlockListListType, List<ParsedBlockListBlockId>>
            {
                {
                    GetBlockListListType.Committed, new List<ParsedBlockListBlockId>()
                },
                {
                    GetBlockListListType.Uncommitted, new List<ParsedBlockListBlockId>
                    {
                        new ParsedBlockListBlockId {Name = "aGVyZQ==", Size = 1024},
                        new ParsedBlockListBlockId {Name = "c2luY2VyZWx5", Size = 9}
                    }
                }
            };

            var parsedXml = Parsers.ParseAllXmlBlockLists(xmlRoot.Root);

            AssertBlockListDictionariesEqual(expectedBlockList, parsedXml);
        }

        private static void AssertBlockListDictionariesEqual(Dictionary<GetBlockListListType, List<ParsedBlockListBlockId>> expectedBlockLists, Dictionary<GetBlockListListType, List<ParsedBlockListBlockId>> parsedXml)
        {
            Assert.True(
            expectedBlockLists.All(expectedBlockList =>
                expectedBlockList.Value.All(expectedBlock =>
                {
                    var parsedBlock = parsedXml[expectedBlockList.Key].FirstOrDefault(_ => _.Name == expectedBlock.Name);

                    Assert.NotNull(parsedBlock);
                    Assert.AreEqual(expectedBlock.Name, parsedBlock.Name);
                    Assert.AreEqual(expectedBlock.Size, parsedBlock.Size);
                    return true;
                })));
        }
    }
}
