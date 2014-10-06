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
        public static string GenerateSharedKeyLiteSignatureStringForTableService(WebRequest request, Dictionary<string, string> queryStringParameters, StorageAccountSettings settings)
        {
            var queryStrings = queryStringParameters.OrderBy(kvp => kvp.Key)
                                                    .Select(kvp => kvp.Key + ":" + kvp.Value);

            string canonicalizedResource = "/" + settings.AccountName +
                                            String.Join("", request.RequestUri.Segments);
            canonicalizedResource = canonicalizedResource.TrimEnd(new char[] { '\n' });

            //SharedKeyLIte
            string stringToSign = String.Format(
                /* Date */ "{0}\n" +
                /* Canonicalized Resource */ "{1}",
                request.Headers[ProtocolConstants.Headers.Date],
                canonicalizedResource);

            return settings.ComputeMacSha256(stringToSign);
        }

        public static string GenerateSharedKeySignatureStringForTableService(WebRequest request, Dictionary<string, string> queryStringParameters, StorageAccountSettings settings)
        {

            var queryStrings = queryStringParameters.OrderBy(kvp => kvp.Key)
                                                    .Select(kvp => kvp.Key + ":" + kvp.Value);

            string canonicalizedResource = "/" + settings.AccountName +
                                            String.Join("", request.RequestUri.Segments);
            canonicalizedResource = canonicalizedResource.TrimEnd(new char[] { '\n' });

            // Shared Key
            string stringToSign = String.Format(
                /* Method */ "{0}\n" +
                /* Content-MD5 */ "{1}\n" +
                /* Content-Type */ "{2}\n" +
                /* Date */ "{3}\n" +
                /* Canonicalized Resource */ "{4}",
                request.Method,
                request.Headers[ProtocolConstants.Headers.ContentMD5],
                request.ContentType,
                request.Headers[ProtocolConstants.Headers.Date],
                canonicalizedResource);

            return settings.ComputeMacSha256(stringToSign);
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

            string contentLength = "";
            if (request.Method == "POST" || request.Method == "PUT" || request.Method == "DELETE")
                contentLength = request.ContentLength.ToString();

            string stringToSign = String.Format(
                /* Method */ "{0}\n"
                /* Content-Encoding */ + "{1}\n"
                /* Content-Language */ + "{2}\n"
                /* Content-Length */ + "{3}\n"
                /* Content-MD5 */ + "{4}\n"
                /* Content-Type */ + "{5}\n"
                /* Date */ + "{6}\n"
                /* If-Modified-Since */ + "{7}\n"
                /* If-Match */ + "{8}\n"
                /* If-None-Match */ + "{9}\n"
                /* If-Unmodified-Since */ + "{10}\n"
                /* Range */ + "{11}\n"
                /* Canonicalized Headers */ + "{12}\n"
                /* Canonicalilzed Resource */ + "{13}",
                request.Method,
                request.Headers[ProtocolConstants.Headers.ContentEncoding],
                request.Headers[ProtocolConstants.Headers.ContentLanguage],
                contentLength,
                request.Headers[ProtocolConstants.Headers.ContentMD5],
                request.ContentType,
                "",
                "",
                "",
                "",
                "",
                "",
                String.Join("\n", canonicalizedHeaders),
                canonicalizedResource);


            return settings.ComputeMacSha256(stringToSign);
        }


    }
}
