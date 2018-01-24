using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Swift.Core
{
    public class DefaultConfiguration
    {
        /// <summary>
        /// 作业路径
        /// </summary>
        public static string JobPath
        {
            get
            {
                return "Jobs" + Path.DirectorySeparatorChar + "{0}";
            }
        }

        /// <summary>
        /// 作业程序运行路径
        /// </summary>
        public static string JobRunTimePath
        {
            get
            {
                return "Jobs" + Path.DirectorySeparatorChar + "{0}" + Path.DirectorySeparatorChar + "runtime";
            }
        }

        /// <summary>
        /// 作业运行记录路径
        /// </summary>
        public static string JobRecordsPath
        {
            get
            {
                return "Jobs" + Path.DirectorySeparatorChar + "{0}" + Path.DirectorySeparatorChar + "records";
            }
        }

        /// <summary>
        /// 作业程序包路径
        /// </summary>
        public static string JobPkgPath
        {
            get
            {
                return "Jobs";
            }
        }

        /// <summary>
        /// 任务文件路径
        /// </summary>
        public static string TaskFilePath
        {
            get
            {
                return "Jobs" + Path.DirectorySeparatorChar + "{0}"  + Path.DirectorySeparatorChar + "{1}" + Path.DirectorySeparatorChar + "tasks" + Path.DirectorySeparatorChar + "{2}";
            }
        }
    }
}
