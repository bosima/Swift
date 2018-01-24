using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Swift.Core.ExtensionException
{
    /// <summary>
    /// 成员不存在异常
    /// </summary>
    public class MemberNotFoundException : System.Exception
    {
        public MemberNotFoundException(string message)
            : base(message)
        {
        }

        public MemberNotFoundException(string message, System.Exception ex)
            : base(message, ex)
        {
        }
    }
}
