using System.Text.RegularExpressions;
using Basic.Azure.Storage.Communications.BlobService;

namespace Basic.Azure.Storage.Communications.Utility
{
    public static class CopyProgressParse
    {
        private const string CopiedGroupName = "copied";
        private const string TotalGroupName = "total";
        private const string RegexMatch = @"^(?<" + CopiedGroupName + @">\d*)\/(?<" + TotalGroupName + @">\d*)$";
        private static readonly Regex Matcher = new Regex(RegexMatch);

        public static BlobCopyProgress ParseCopyProgress(string copyProgress)
        {
            if (null == copyProgress)
                return null;

            var match = Matcher.Match(copyProgress);

            if (!match.Success)
                return null;

            var copied = int.Parse(match.Groups[CopiedGroupName].Value);
            var total = int.Parse(match.Groups[TotalGroupName].Value);
            return new BlobCopyProgress(copied, total);
        }
    }
}