using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Swift.Core.Log
{
    /// <summary>
    /// 日志记录类
    /// </summary>
    internal class LogWriter
    {
        private static NLog.ILogger logger;

        static LogWriter()
        {
            logger = NLog.LogManager.GetLogger("SwiftLogger");
        }

        public static void Write(string message)
        {
            Write(message, LogLevel.Info);
        }

        public static void Write(string message, LogLevel level)
        {
            Console.WriteLine(string.Format("{0} [{1}] {2}", DateTime.Now.ToString(), level.ToString(), message));

            switch (level)
            {
                case LogLevel.Trace:
                    logger.Trace(message);
                    break;
                case LogLevel.Debug:
                    logger.Debug(message);
                    break;
                case LogLevel.Info:
                    logger.Info(message);
                    break;
                case LogLevel.Warn:
                    logger.Warn(message);
                    break;
                case LogLevel.Error:
                    logger.Error(message);
                    break;
            }
        }

        public static void Write(string message, Exception ex)
        {
            Write(message, ex, LogLevel.Error);
        }

        public static void Write(string message, Exception ex, LogLevel level)
        {
            Console.WriteLine(string.Format("{0} [{1}] {2}", DateTime.Now.ToString(), level.ToString(), message));
            if (ex != null)
            {
                Console.WriteLine(string.Format("{0} [{1}] {2}", DateTime.Now.ToString(), level.ToString(), ex.Message));
                Console.WriteLine(string.Format("{0} [{1}] {2}", DateTime.Now.ToString(), level.ToString(), ex.StackTrace));
            }

            switch (level)
            {
                case LogLevel.Trace:
                    logger.Trace(ex, message);
                    break;
                case LogLevel.Debug:
                    logger.Debug(ex, message);
                    break;
                case LogLevel.Info:
                    logger.Info(ex, message);
                    break;
                case LogLevel.Warn:
                    logger.Warn(ex, message);
                    break;
                case LogLevel.Error:
                    logger.Error(ex, message);
                    break;
            }
        }
    }
}
