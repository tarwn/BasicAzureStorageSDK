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

        //TODO: implement a validation check and exception for Identifier rules

        public static void EnsureNameIsValidIdentifier(string name)
        {
            if (!IsValidIdentifier(name))
            {
                throw GenerateExceptionForInvalidName(name);
            }
        }

        public static void EnsureNamesAreValidIdentifiers(IEnumerable<string> names)
        {
            const string invalidIdentifiers = "The provided list of names contains invalid identifiers.";

            var invalidNames = names
                .Where(n => !IsValidIdentifier(n))
                .ToList();

            if (invalidNames.Any())
            {
                throw new AggregateException(invalidIdentifiers, invalidNames.Select(GenerateExceptionForInvalidName));
            }
        }

        private static bool IsValidIdentifier(string name)
        {
            return Provider.IsValidIdentifier(name);
        }

        private static ArgumentException GenerateExceptionForInvalidName(string name)
        {
            const string invalidIdentifier = "The provided identifier [{0}] is not a valid identifier.";

            return new ArgumentException(String.Format(invalidIdentifier, name));
        }

    }
}
