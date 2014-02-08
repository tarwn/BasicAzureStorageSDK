using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TestableHttpWebResponse.ResponseSettings
{
	public class ExceptionResponseSettings : BaseResponseSettings
	{
		public Exception ExceptionToThrow { get; set; }

		public ExceptionResponseSettings(Exception exceptionToThrow)
		{
			ExceptionToThrow = exceptionToThrow;
			ExpectException = true;
		}
	}
}
