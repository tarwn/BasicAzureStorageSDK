using System;
using System.Text;

namespace Basic.Azure.Storage.Communications.Utility
{
    public static class Base64Converter
    {
        public static string ConvertToBase64(string original)
        {
            return Convert.ToBase64String(Encoding.Unicode.GetBytes(original));
        }

        public static string ConvertFromBase64(string base64)
        {
            return Encoding.Unicode.GetString(Convert.FromBase64String(base64));
        }
    }
}

