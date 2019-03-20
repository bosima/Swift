using System;
using System.Diagnostics;

namespace Swift.Core.OS.OSX
{
    public class OSXAPI : IOSAPI
    {
        public OSXAPI()
        {
        }

        public string GetProcessCommmandLine(Process process)
        {
            // https://superuser.com/questions/27748/how-to-get-command-line-of-unix-process

            return Bash("ps -p " + process.Id + " -o command=");
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
