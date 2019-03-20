using System;
using System.Diagnostics;

namespace Swift.Core.OS.Linux
{
    public class LinuxAPI : IOSAPI
    {
        public string GetProcessCommmandLine(Process process)
        {
            // https://stackoverflow.com/questions/821837/how-to-get-the-command-line-args-passed-to-a-running-process-on-unix-linux-syste

            return Bash("xargs -0 < /proc/" + process.Id + "/cmdline");
        }

        /// <summary>
        /// 执行命令
        /// </summary>
        /// <returns>The bash.</returns>
        /// <param name="cmd">Cmd.</param>
        private string Bash(string cmd)
        {
            var escapedArgs = cmd.Replace("\"", "\\\"");

            var process = new Process()
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "/bin/bash",
                    Arguments = $"-c \"{escapedArgs}\"",
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                }
            };
            process.Start();
            string result = process.StandardOutput.ReadToEnd();
            process.WaitForExit();
            return result.TrimEnd(Environment.NewLine.ToCharArray()).Trim();
        }

    }
}
