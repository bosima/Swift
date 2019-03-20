using System;
namespace Swift.Core
{
    /// <summary>
    /// 可执行文件类型
    /// </summary>
    [Flags]
    public enum EnumExecutableFileType
    {
        /// <summary>
        /// 直接可执行文件
        /// </summary>
        DirectExe = 1,

        /// <summary>
        /// dotnet core
        /// </summary>
        DotNet = 2,
    }
}
