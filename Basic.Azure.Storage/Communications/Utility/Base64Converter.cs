using System;
using System.Text;

namespace Basic.Azure.Storage.Communications.Utility
{
    public static class Base64Converter
    {
        public static string ConvertToBase64(string original)
        {
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(original));
        }

        public static string ConvertFromBase64(string base64)
        {
            return Encoding.UTF8.GetString(Convert.FromBase64String(base64));
        }
    }
}
