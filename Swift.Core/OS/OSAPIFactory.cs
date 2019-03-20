using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Swift.Core.OS.Linux;
using Swift.Core.OS.OSX;
using Swift.Core.OS.Windows;

namespace Swift.Core.OS
{
    internal static class OSAPIFactory
    {
        public static IOSAPI Create()
        {
            var os = GetOSPlatform();
            if (os == OSPlatform.Windows)
            {
                return new WindowsAPI();
            }
            else if (os == OSPlatform.Linux)
            {
                return new LinuxAPI();
            }
            else if (os == OSPlatform.OSX)
            {
                return new OSXAPI();
            }

            throw new PlatformNotSupportedException("不支持此操作系统");
        }

        /// <summary> 
        /// Get OS platform
        /// https://code.msdn.microsoft.com/How-to-determine-operating-c90d351b#content
        /// </summary> 
        /// <returns></returns> 
        public static OSPlatform GetOSPlatform()
        {
            OSPlatform osPlatform = OSPlatform.Create("Other Platform");
            // Check if it's windows 
            bool isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
            osPlatform = isWindows ? OSPlatform.Windows : osPlatform;
            // Check if it's osx 
            bool isOSX = RuntimeInformation.IsOSPlatform(OSPlatform.OSX);
            osPlatform = isOSX ? OSPlatform.OSX : osPlatform;
            // Check if it's Linux 
            bool isLinux = RuntimeInformation.IsOSPlatform(OSPlatform.Linux);
            osPlatform = isLinux ? OSPlatform.Linux : osPlatform;
            return osPlatform;
        }
    }
}
