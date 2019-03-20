using System;
using System.Diagnostics;
using System.Management;

namespace Swift.Core.OS.Windows
{
    public class WindowsAPI : IOSAPI
    {
        public string GetProcessCommmandLine(Process process)
        {
            using (ManagementObjectSearcher searcher = new ManagementObjectSearcher("SELECT CommandLine FROM Win32_Process WHERE ProcessId = " + process.Id))
            using (ManagementObjectCollection objects = searcher.Get())
            {
                String commandLine = "";
                foreach (ManagementObject commandLineObject in objects)
                {
                    commandLine += (String)commandLineObject["CommandLine"];
                }

                return commandLine.TrimEnd(Environment.NewLine.ToCharArray()).Trim();
            }
        }
    }
}
