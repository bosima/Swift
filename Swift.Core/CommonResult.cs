using System;
namespace Swift.Core
{
    /// <summary>
    /// 通用结果
    /// </summary>
    public class CommonResult
    {
        public CommonResult()
        {
        }

        public int ErrCode { get; set; }

        public string ErrMessage { get; set; }
    }
}
