using System;
using System.Collections.Generic;
using System.IO;
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

        internal static void ArgumentIsBase64Encoded(string parameterName, string parameterValue)
        {
            try
            {
                Convert.FromBase64String(parameterValue);
            }
            catch (Exception ex)
            {
                throw new ArgumentException(String.Format("The provided data {0} is not Base64 encoded", parameterValue), parameterName, ex);
            }
        }

        internal static void ArgumentArrayLengthIsEqualOrSmallerThanSize(string parameterName, Array parameterValue, int size)
        {
            if (parameterValue.Length > size)
            {
                throw new ArgumentException(String.Format("The provided array is longer than maximum size {0}.", size), parameterName);
            }
        }

        internal static void ArgumentIsNotNull(string parameterName, object parameterValue)
        {
            if (null == parameterValue)
            {
                throw new ArgumentNullException(parameterName, String.Format("The provided value {0} is null", parameterName));
            }
        }

        internal static void StreamIsReadable(string parameterName, Stream parameterValue)
        {
            if (!parameterValue.CanRead)
            {
                throw new ArgumentException(String.Format("The provided stream {0} is not readable.", parameterName));
            }
        }

    }
}
