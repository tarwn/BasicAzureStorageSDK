using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Core
{
    public static class SignedAuthorization
    {

        public static string GenerateSharedKeySignatureStringForTableService(HttpWebRequest request, StorageAccountSettings settings)
        {
            return "";
        }

        public static string GenerateSharedKeySignatureString(WebRequest request, Dictionary<string,string> queryStringParameters, StorageAccountSettings settings)
        {
            var canonicalizedHeaders = request.Headers.AllKeys
                .Where(k => k.StartsWith("x-ms-"))
                .OrderBy(k => k.ToLower())
                .Select(k => k.ToLower() + ":" + request.Headers[k].Replace('\r',' ').Replace('\n',' ').TrimStart());

            var queryStrings = queryStringParameters.OrderBy(kvp => kvp.Key)
            .Select(kvp => kvp.Key + ":" + kvp.Value);

            string canonicalizedResource =
                    "/" + settings.AccountName +
                    String.Join("", request.RequestUri.Segments) + "\n" +
                    String.Join("\n", queryStrings);
            canonicalizedResource = canonicalizedResource.TrimEnd(new char[] { '\n' });

            string stringToSign =
                    request.Method + "\n"
                    /* Content-Encoding */ + "\n"
                    /* Content-Language */ + "\n"
                    /* Content-Length */ + "0\n"
                    /* Content-MD5 */ + "\n"
                    /* Content-Type */ + "\n"
                    /* Date */ + "\n"
                    /* If-Modified-Since */ + "\n"
                    /* If-Match */ + "\n"
                    /* If-None-Match */ + "\n"
                    /* If-Unmodified-Since */ + "\n"
                    /* Range */ + "\n"
                    + String.Join("\n", canonicalizedHeaders) + "\n"
                    + canonicalizedResource;

            return settings.ComputeMacSha256(stringToSign);
        }

    }
}
