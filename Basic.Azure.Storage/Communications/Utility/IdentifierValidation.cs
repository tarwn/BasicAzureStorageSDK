using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CSharp;

namespace Basic.Azure.Storage.Communications.Utility
{
    /// <summary>
    /// Ensures a name or collection of names meets the rules for C# Identifiers
    /// http://msdn.microsoft.com/en-us/library/aa664670(v=vs.71).aspx
    /// </summary>
    /// <remarks>
    /// This is required for metadata names in Azure requests
    /// </remarks>
    public static class IdentifierValidation
    {
        // http://stackoverflow.com/a/1904361 - Not the fastest way, but the most correct. If speed is an issue we can refactor.
        private static readonly CSharpCodeProvider Provider = new CSharpCodeProvider();
        
        public static void EnsureNamesAreValidIdentifiers(IEnumerable<string> names)
        {
            const string invalidIdentifiers = "The provided list of names contains invalid identifiers.";
            const string invalidIdentifier = "The provided identifier [{0}] is not a valid identifier.";

            var invalidNames = names
                .Where(n => !Provider.IsValidIdentifier(n))
                .ToList();

            if (invalidNames.Any())
            {
                throw new AggregateException(invalidIdentifiers, invalidNames.Select(n => new ArgumentException(string.Format(invalidIdentifier, n))));
            }
        }

    }
}
