using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Swift.Core.ExtensionException
{
    public class JobPackageConfigExtractException : System.Exception
    {
        public JobPackageConfigExtractException(string message)
            : base(message)
        {
        }

        public JobPackageConfigExtractException(string message, System.Exception ex)
            : base(message, ex)
        {
        }
    }
}

