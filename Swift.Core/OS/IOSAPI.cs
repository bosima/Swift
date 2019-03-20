using System;
using System.Diagnostics;

namespace Swift.Core.OS
{
    /// <summary>
    /// 操作系统API接口
    /// </summary>
    public interface IOSAPI
    {
        /// <summary>
        /// 获取进程启动时的命令行
        /// </summary>
        /// <returns>The process commmand line.</returns>
        /// <param name="process">Process.</param>
        string GetProcessCommmandLine(Process process);
    }
}
