using System;
using System.Runtime.Serialization;
using System.Security.Permissions;

namespace ConsoleApp1.Model
{
    [Serializable]
    public class InvalidCompareException : Exception {
        public InvalidCompareException()
        {
        }

        public InvalidCompareException(string message) : base(message) { }

        public InvalidCompareException(string message, Exception inner) : base(message, inner) { }
        protected InvalidCompareException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
