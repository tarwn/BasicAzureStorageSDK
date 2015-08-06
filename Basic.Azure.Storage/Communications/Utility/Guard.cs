using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Utility
{
    public class Guard
    {
        public static void ArgumentInRanges<T>(string parameterName, T parameterValue, GuardRange<T> allowedRange)
           where T : IComparable
        {
            ArgumentInRanges<T>(parameterName, parameterValue, new GuardRange<T>[] { allowedRange });
        }

        public static void ArgumentInRanges<T>(string parameterName, T parameterValue, GuardRange<T>[] allowedRanges)
           where T : IComparable
        {
            foreach (var range in allowedRanges)
            {
                if (range.IsInRange(parameterValue))
                    return;
            }

            throw new ArgumentOutOfRangeException(parameterName, String.Format("The value {0} is not in the list of allowed values: {1}", parameterValue, String.Join(",", allowedRanges.Select(ar => ar.GetText()))));
        }

        public static void ArgumentGreaterThan<T>(string parameterName, T parameterValue, string minimumParameterName, T minimumParameterValue)
            where T : IComparable
        {
            if (parameterValue.CompareTo(minimumParameterValue) <= 0)
            {
                throw new ArgumentOutOfRangeException(parameterName, String.Format("The value {0} for parameter {1} must be greater than the value for parameter {2} (currently {3})", parameterValue, parameterName, minimumParameterName, minimumParameterValue));
            }
        }

        internal static void ArgumentIsAGuid(string parameterName, string parameterValue)
        {
            Guid g;
            if (!Guid.TryParse(parameterValue, out g))
            {
                throw new ArgumentException(String.Format("Cannot convert {0} to a Guid", parameterValue), parameterName);
            }
        }
        
        internal static void ArgumentIsNotNullOrEmpty(string parameterName, string parameterValue)
        {
            if (string.IsNullOrEmpty(parameterValue))
            {
                throw new ArgumentNullException(parameterName, String.Format("The provided {0} parameter is null or empty", parameterName));
            }
        }

    }
}
