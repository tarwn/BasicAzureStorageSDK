using System;
using System.Globalization;

namespace Basic.Azure.Storage.Communications.Utility
{
    public static class DateParse
    {

        public static DateTime ParseUTC(string dateIn8601)
        {
            // Per Jon Skeet: http://stackoverflow.com/questions/10029099/datetime-parse2012-09-30t230000-0000000z-always-converts-to-datetimekind-l
            return DateTime.ParseExact(dateIn8601,
                                        "yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'",
                                        CultureInfo.InvariantCulture,
                                        DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
        }

        public static DateTime ParseHeader(string headerValue)
        {
            DateTime dateValue;
            DateTime.TryParse(headerValue, out dateValue);
            return dateValue;
        }
    }
}
