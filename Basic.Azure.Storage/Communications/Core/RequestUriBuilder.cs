using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Core
{
    public class RequestUriBuilder
    {
        private string _baseUrl;
        private List<string> _segments;
        private Dictionary<string, string> _parameters;

        public RequestUriBuilder(string baseUrl)
        {
            _baseUrl = GetSegmentWithoutTrailingSlash(baseUrl);
            _segments = new List<string>();
            _parameters = new Dictionary<string, string>();
        }

        public void AddSegment(string urlSegment)
        {
            _segments.Add(GetSegmentWithoutTrailingSlash(urlSegment));
        }

        private string GetSegmentWithoutTrailingSlash(string urlSegment)
        {
            if (urlSegment.EndsWith("/"))
                return urlSegment.Substring(0, _baseUrl.Length - 1);
            else
                return urlSegment;
        }

        public Dictionary<string, string> GetParameters()
        {
            return _parameters;
        }

        public Uri GetUri()
        {
            string rawUrl;
            if (_parameters.Count == 0)
            {
                rawUrl = String.Format("{0}/{1}", _baseUrl, String.Join("/", _segments));
            }
            else
            {
                rawUrl = String.Format("{0}/{1}?{2}",
                    _baseUrl,
                    String.Join("/", _segments),
                    String.Join("&", _parameters.Select(kvp => String.Format("{0}={1}", kvp.Key, kvp.Value))));
            }

            var uri = new Uri(rawUrl);
            return uri;
        }

    }
}
