using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Swift.Core
{
    public class SwiftConfiguration
    {
        /// <summary>
        /// 刷新集群成员间隔时间，单位毫秒
        /// </summary>
        public static int RefreshMemberInterval
        {
            get
            {
                return 5000;
            }
        }

        /// <summary>
        /// 刷新作业配置间隔时间，单位毫秒
        /// </summary>
        public static int RefreshJobConfigInterval
        {
            get
            {
                return 10000;
            }
        }

        /// <summary>
        /// 作业空间创建间隔时间，单位毫秒
        /// </summary>
        public static int JobSpaceCreateInterval
        {
            get
            {
                return 15000;
            }
        }

        /// <summary>
        /// 刷新作业间隔时间，单位毫秒
        /// </summary>
        public static int RefreshJobInterval
        {
            get
            {
                return 8000;
            }
        }

        /// <summary>
        /// 当前Swift的根路径
        /// </summary>
        public static string BaseDirectory
        {
            get
            {
                return System.AppDomain.CurrentDomain.SetupInformation.ApplicationBase;
            }
        }
    }
}
