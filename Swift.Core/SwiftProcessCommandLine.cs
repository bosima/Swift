using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Swift.Core.Log;
using Swift.Core.OS;

namespace Swift.Core
{
    /// <summary>
    /// Swift进程命令行
    /// </summary>
    public class SwiftProcessCommandLine
    {
        /// <summary>
        /// 可执行文件类型
        /// </summary>
        /// <value>The type of the executable file.</value>
        public EnumExecutableFileType ExecutableFileType
        {
            get;
            set;
        }

        /// <summary>
        /// 可执行文件名称
        /// </summary>
        /// <value>The name of the file.</value>
        public string FileName
        {
            get;
            set;
        }

        /// <summary>
        /// 参数字典
        /// </summary>
        /// <value>The paras.</value>
        public Dictionary<string, string> Paras
        {
            get;
            set;
        }

        /// <summary>
        /// 原始命令行
        /// </summary>
        /// <value>The orignal.</value>
        public string Orignal
        {
            get;
            set;
        }

        /// <summary>
        /// 获取进程的原始命令行
        /// </summary>
        /// <returns>The orignal command line from process.</returns>
        public static string GetOrignal(Process process, CancellationToken cancellationToken = default(CancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var osAPI = OSAPIFactory.Create();
            return osAPI.GetProcessCommmandLine(process);
        }

        /// <summary>
        /// 获取进程的格式化命令行
        /// </summary>
        /// <returns>The get.</returns>
        /// <param name="process">Process.</param>
        /// <param name="executableFileType">Executable file type.</param>
        public static SwiftProcessCommandLine Get(Process process, EnumExecutableFileType executableFileType, CancellationToken cancellationToken = default(CancellationToken))
        {
            var commandLine = GetOrignal(process, cancellationToken);
            LogWriter.Write("原始命令行为：" + commandLine, LogLevel.Debug);

            if (string.IsNullOrWhiteSpace(commandLine))
            {
                return null;
            }

            cancellationToken.ThrowIfCancellationRequested();

            if ((executableFileType & EnumExecutableFileType.DirectExe) == EnumExecutableFileType.DirectExe)
            {
                if (TryFormatWithDirectExe(commandLine, out Tuple<string, string> directExeCommandLine))
                {
                    return new SwiftProcessCommandLine()
                    {
                        ExecutableFileType = EnumExecutableFileType.DirectExe,
                        FileName = directExeCommandLine.Item1,
                        Paras = ResolveArguments(directExeCommandLine.Item2.Split(' ')),
                        Orignal = commandLine
                    };
                }
            }

            cancellationToken.ThrowIfCancellationRequested();

            if ((executableFileType & EnumExecutableFileType.DotNet) == EnumExecutableFileType.DotNet)
            {
                if (TryFormatWithDotNet(commandLine, out Tuple<string, string> dotnetCommandLine))
                {
                    return new SwiftProcessCommandLine()
                    {
                        ExecutableFileType = EnumExecutableFileType.DotNet,
                        FileName = dotnetCommandLine.Item1,
                        Paras = ResolveArguments(dotnetCommandLine.Item2.Split(' ')),
                        Orignal = commandLine
                    };
                }
            }

            throw new NotSupportedException("the format of process's commandline not supported: " + commandLine);
        }

        /// <summary>
        /// Tries the format with windows exe.
        /// </summary>
        /// <returns><c>true</c>, if format with windows exe was tryed, <c>false</c> otherwise.</returns>
        /// <param name="commandLine">Command line.</param>
        /// <param name="paras">Paras.</param>
        private static bool TryFormatWithDirectExe(string commandLine, out Tuple<string, string> paras)
        {
            paras = null;
            var processFileName = string.Empty;
            var processParas = string.Empty;

            commandLine = commandLine.Trim();
            var fileNameLength = commandLine.IndexOf(' ');
            if (fileNameLength > 0)
            {
                processFileName = commandLine.Substring(0, fileNameLength);
                LogWriter.Write("发现命令行第1部分：" + processFileName, LogLevel.Trace);

                processParas = commandLine.Substring(fileNameLength + 1).Trim();
                LogWriter.Write("发现命令行第2部分：" + processParas, LogLevel.Trace);

                if (processParas.StartsWith("-", StringComparison.Ordinal))
                {
                    paras = new Tuple<string, string>(processFileName, processParas);
                    return true;
                }
                LogWriter.Write("命令行非DirectExe类型", LogLevel.Trace);
            }

            return false;
        }

        /// <summary>
        /// Tries the format with dotnet.
        /// </summary>
        /// <returns><c>true</c>, if format with dot net was tryed, <c>false</c> otherwise.</returns>
        /// <param name="commandLine">Command line.</param>
        /// <param name="paras">Paras.</param>
        private static bool TryFormatWithDotNet(string commandLine, out Tuple<string, string> paras)
        {
            paras = null;
            var processFileName = string.Empty;
            var processParas = string.Empty;

            commandLine = commandLine.Trim();
            var dotnetNameLength = commandLine.IndexOf(' ');
            if (dotnetNameLength > 0)
            {
                var dotnetName = commandLine.Substring(0, dotnetNameLength);
                LogWriter.Write("发现命令行第1部分：" + dotnetName, LogLevel.Trace);

                if (dotnetName == "dotnet")
                {
                    var programCommandLine = commandLine.Substring(dotnetNameLength + 1);
                    LogWriter.Write("发现命令行第2部分：" + programCommandLine, LogLevel.Trace);

                    var fileNameLength = programCommandLine.IndexOf(' ');
                    if (fileNameLength > 0)
                    {
                        processFileName = programCommandLine.Substring(0, fileNameLength);
                        processParas = programCommandLine.Substring(fileNameLength + 1).Trim();
                        LogWriter.Write("发现命令行第3部分：" + processParas, LogLevel.Trace);

                        if (processParas.StartsWith("-", StringComparison.Ordinal))
                        {
                            paras = new Tuple<string, string>(processFileName, processParas);
                            return true;
                        }
                        LogWriter.Write("命令行非DotNet类型", LogLevel.Trace);
                    }
                }
            }

            return false;
        }

        /// <summary>
        /// 解析启动参数
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        private static Dictionary<string, string> ResolveArguments(string[] args)
        {
            Dictionary<string, string> paras = new Dictionary<string, string>();

            for (int i = 0; i < args.Length; i++)
            {
                if (!args[i].StartsWith("-", StringComparison.Ordinal))
                {
                    continue;
                }

                var key = args[i].ToLower();

                if (string.IsNullOrWhiteSpace(key))
                {
                    continue;
                }

                var val = string.Empty;
                if (i + 1 < args.Length)
                {
                    if (!args[i + 1].StartsWith("-", StringComparison.Ordinal))
                    {
                        val = args[i + 1].Trim();
                        i = i + 1;
                    }
                }

                paras.Add(key, val);
            }

            return paras;
        }
    }
}
