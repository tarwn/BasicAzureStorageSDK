using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Utility
{
    public class DateParse
    {

        public static DateTime Parse(string dateIn8601)
        {
            // Per Jon Skeet: http://stackoverflow.com/questions/10029099/datetime-parse2012-09-30t230000-0000000z-always-converts-to-datetimekind-l
            return DateTime.ParseExact(dateIn8601,
                                        "yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'",
                                        CultureInfo.InvariantCulture,
                                        DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
        }
    }
}
