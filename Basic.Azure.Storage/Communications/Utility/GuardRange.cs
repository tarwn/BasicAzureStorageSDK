using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Basic.Azure.Storage.Communications.Utility
{
    public class GuardRange<T>
        where T : IComparable
    {
        private bool _singleValue = false;

        public T StartValue { get; protected set; }

        public T EndValue { get; protected set; }

        public GuardRange(T individualValue)
        {
            StartValue = individualValue;
            EndValue = individualValue;
            _singleValue = true;
        }

        public GuardRange(T startValue, T endValue)
        {
            StartValue = startValue;
            EndValue = endValue;
            _singleValue = false;
        }

        public bool IsInRange(T value)
        {
            return StartValue.CompareTo(value) <= 0 && EndValue.CompareTo(value) >= 0;
        }
        
        public string GetText()
        {
            if (_singleValue)
            {
                return StartValue.ToString();
            }
            else
            {
                return String.Format("{0} to {1}", StartValue, EndValue);
            }
        }
    }
}
