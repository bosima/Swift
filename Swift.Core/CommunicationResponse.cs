using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Swift.Core
{
    /// <summary>
    /// 成员通信响应
    /// </summary>
    public class CommunicationResponse
    {
        public int ErrCode
        {
            get;
            set;
        }

        public string ErrMsg
        {
            get;
            set;
        }
    }
}
