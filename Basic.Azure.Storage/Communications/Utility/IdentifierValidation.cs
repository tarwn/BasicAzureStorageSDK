using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        //TODO: implement a validation check and exception for Identifier rules

        public static void EnsureNameIsValidIdentifier(string name)
        { 
            // add some logic
        }

        public static void EnsureNamesAreValidIdentifiers(IEnumerable<string> names)
        { 
            // add some logic
        }

    }
}
