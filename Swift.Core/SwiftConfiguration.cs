using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Swift.Core
{
    public static class SwiftConfiguration
    {
        #region 时间阈值
        /// <summary>
        /// 检查集群成员健康状态间隔时间，单位毫秒
        /// </summary>
        public static int CheckMemberInterval
        {
            get
            {
                return 6000;
            }
        }

        /// <summary>
        /// 刷新作业配置间隔时间，单位毫秒
        /// </summary>
        public static int RefreshJobConfigsInterval
        {
            get
            {
                return 9000;
            }
        }

        /// <summary>
        /// 作业空间创建间隔时间，单位毫秒
        /// </summary>
        public static int JobSpaceCreateInterval
        {
            get
            {
                return 12000;
            }
        }
        #endregion

        #region 物理路径
        /// <summary>
        /// 当前Swift的磁盘物理路径
        /// </summary>
        public static string BaseDirectory
        {
            get
            {
                string baseDirectory = System.AppDomain.CurrentDomain.BaseDirectory;

                // 如果是从作业实例目录启动的，需要截取下
                if (baseDirectory.IndexOf("Jobs", StringComparison.Ordinal) >= 0)
                {
                    return baseDirectory.Substring(0, baseDirectory.IndexOf("Jobs", StringComparison.Ordinal));
                }

                return baseDirectory;
            }
        }

        /// <summary>
        /// Swift实例下全部作业根路径
        /// </summary>
        public static string AllJobRootPath
        {
            get
            {
                return Path.Combine(BaseDirectory, "Jobs");
            }
        }

        /// <summary>
        /// Swift实例下全部执行进程文件的根路径
        /// </summary>
        public static string AllSwiftProcessRootPath
        {
            get
            {
                return Path.Combine(BaseDirectory, "Jobs", "process");
            }
        }

        /// <summary>
        /// 指定进程文件的路径
        /// </summary>
        public static string GetSwiftProcessPath(string method, string businessId)
        {
            string fileName = string.Format("{0}-{1}.p", method, businessId);
            return Path.Combine(AllSwiftProcessRootPath, fileName);
        }

        /// <summary>
        /// 获取制定作业的物理路径
        /// </summary>
        public static string GetJobRootPath(string jobName)
        {

            return Path.Combine(AllJobRootPath, jobName);
        }

        /// <summary>
        /// 获取指定作业包所有文件的物理路径
        /// </summary>
        /// <returns>The job record root path.</returns>
        /// <param name="jobName">作业名称</param>
        public static string GetJobPackageRootPath(string jobName)
        {
            return Path.Combine(GetJobRootPath(jobName), "packages");
        }

        /// <summary>
        /// 获取指定作业包的物理路径
        /// </summary>
        /// <returns>The job record root path.</returns>
        /// <param name="jobName">作业名称</param>
        /// <param name="jobVersion">作业版本</param>
        public static string GetJobPackagePath(string jobName, string jobVersion)
        {
            return Path.Combine(GetJobPackageRootPath(jobName), jobVersion + ".zip");
        }

        /// <summary>
        /// 获取指定作业所有运行程序文件的物理路径
        /// </summary>
        /// <returns>The job program root path.</returns>
        /// <param name="jobName">作业名称</param>
        public static string GetJobProgramRootPath(string jobName)
        {
            return Path.Combine(GetJobRootPath(jobName), "programs");
        }

        /// <summary>
        /// 获取指定作业指定版本程序文件的物理路径
        /// </summary>
        /// <returns>The job program path.</returns>
        /// <param name="jobName">作业名称</param>
        /// <param name="jobVersion">作业版本</param>
        public static string GetJobProgramPath(string jobName, string jobVersion)
        {
            return Path.Combine(GetJobProgramRootPath(jobName), jobVersion);
        }

        /// <summary>
        /// 获取作业记录的物理路径
        /// </summary>
        /// <returns>The job record root path.</returns>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        public static string GetJobRecordRootPath(string jobName, string jobId)
        {
            string year = jobId.Substring(0, 4);
            string month = jobId.Substring(4, 2);
            string day = jobId.Substring(6, 2);
            return Path.Combine(GetJobRootPath(jobName), "records", year, month, day, jobId);
        }

        /// <summary>
        /// 获取作业记录配置文件的物理路径
        /// </summary>
        /// <returns>The job record root path.</returns>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        public static string GetJobRecordConfigPath(string jobName, string jobId)
        {
            return Path.Combine(GetJobRecordRootPath(jobName, jobId), "job.json");
        }

        /// <summary>
        /// 获取作业记录配置文件的物理路径
        /// </summary>
        /// <returns>The job record root path.</returns>
        /// <param name="jobRecordRootPath">Job name.</param>
        public static string GetJobRecordConfigPath(string jobRecordRootPath)
        {
            return Path.Combine(jobRecordRootPath, "job.json");
        }

        /// <summary>
        /// 获取作业结果文件的物理路径
        /// </summary>
        /// <returns>The job result path.</returns>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        public static string GetJobResultPath(string jobName, string jobId)
        {
            return Path.Combine(GetJobRecordRootPath(jobName, jobId), "result.txt");
        }

        /// <summary>
        /// 获取作业结果文件的物理路径
        /// </summary>
        /// <returns>The job result path.</returns>
        /// <param name="jobRecordRootPath">作业记录的根路径</param>
        public static string GetJobResultPath(string jobRecordRootPath)
        {
            return Path.Combine(jobRecordRootPath, "result.txt");
        }

        /// <summary>
        /// 获取作业结果打包文件的磁盘物理路径
        /// </summary>
        /// <returns>The job result path.</returns>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        public static string GetJobResultPackagePath(string jobName, string jobId)
        {
            return Path.Combine(GetJobRecordRootPath(jobName, jobId), "result.zip");
        }

        /// <summary>
        /// 获取作业结果打包文件的磁盘物理路径
        /// </summary>
        /// <returns>The job result path.</returns>
        /// <param name="jobRecordRootPath">作业记录的根路径</param>
        public static string GetJobResultPackagePath(string jobRecordRootPath)
        {
            return Path.Combine(jobRecordRootPath, "result.zip");
        }

        /// <summary>
        /// 获取作业任务的物理路径
        /// </summary>
        /// <returns>The job task root path.</returns>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        /// <param name="taskId">Task identifier.</param>
        public static string GetJobTaskRootPath(string jobName, string jobId, int taskId)
        {
            return Path.Combine(GetJobRecordRootPath(jobName, jobId), "tasks", taskId.ToString());
        }

        /// <summary>
        /// 获取作业所有任务的根物理路径
        /// </summary>
        /// <returns>The job task root path.</returns>
        /// <param name="jobRecordRootPath">Job Record root path.</param>
        public static string GetJobAllTaskRootPath(string jobRecordRootPath)
        {
            return Path.Combine(jobRecordRootPath, "tasks");
        }

        /// <summary>
        /// 获取作业任务的物理路径
        /// </summary>
        /// <returns>The job task root path.</returns>
        /// <param name="jobRecordRootPath">Job Record root path.</param>
        /// <param name="taskId">Task identifier.</param>
        public static string GetJobTaskRootPath(string jobRecordRootPath, int taskId)
        {
            return Path.Combine(jobRecordRootPath, "tasks", taskId.ToString());
        }

        /// <summary>
        /// 获取作业任务配置的物理路径
        /// </summary>
        /// <returns>The job task config path.</returns>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        /// <param name="taskId">Task identifier.</param>
        public static string GetJobTaskConfigPath(string jobName, string jobId, int taskId)
        {
            return Path.Combine(GetJobRecordRootPath(jobName, jobId), "tasks", taskId.ToString(), "task.json");
        }

        /// <summary>
        /// 获取作业任务配置的物理路径
        /// </summary>
        /// <param name="taskPath">Task Path.</param>
        public static string GetJobTaskConfigPath(string taskPath)
        {
            return Path.Combine(taskPath, "task.json");
        }

        /// <summary>
        /// 获取作业任务需求文件的物理路径
        /// </summary>
        /// <returns>The job task requirement path.</returns>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        /// <param name="taskId">Task identifier.</param>
        public static string GetJobTaskRequirementPath(string jobName, string jobId, int taskId)
        {
            return Path.Combine(GetJobTaskRootPath(jobName, jobId, taskId), "requirement.txt");
        }

        /// <summary>
        /// 获取作业任务需求的物理路径
        /// </summary>
        /// <returns>The job task requirement path.</returns>
        /// <param name="taskPath">Task path.</param>
        public static string GetJobTaskRequirementPath(string taskPath)
        {
            return Path.Combine(taskPath, "requirement.txt");
        }

        /// <summary>
        /// 获取作业任务执行状态文件的物理路径
        /// </summary>
        /// <returns>The job task execute status path.</returns>
        /// <param name="jobRecordRootPath">Job record path.</param>
        /// <param name="taskId">Task identifier.</param>
        public static string GetJobTaskExecuteStatusPath(string jobRecordRootPath, int taskId)
        {
            return Path.Combine(GetJobTaskRootPath(jobRecordRootPath, taskId), "taskexecute.status");
        }

        /// <summary>
        /// 获取作业任务执行状态文件的物理路径
        /// </summary>
        /// <returns>The job task execute status path.</returns>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        /// <param name="taskId">Task identifier.</param>
        public static string GetJobTaskExecuteStatusPath(string jobName, string jobId, int taskId)
        {
            return Path.Combine(GetJobTaskRootPath(jobName, jobId, taskId), "taskexecute.status");
        }

        /// <summary>
        /// 获取作业任务执行状态文件的物理路径
        /// </summary>
        /// <returns>The job task requirement path.</returns>
        /// <param name="taskPath">Task path.</param>
        public static string GetJobTaskExecuteStatusPath(string taskPath)
        {
            return Path.Combine(taskPath, "taskexecute.status");
        }

        /// <summary>
        /// 获取作业任务结果文件的物理路径
        /// </summary>
        /// <returns>The job task result path.</returns>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        /// <param name="taskId">Task identifier.</param>
        public static string GetJobTaskResultPath(string jobName, string jobId, int taskId)
        {
            return Path.Combine(GetJobTaskRootPath(jobName, jobId, taskId), "result.txt");
        }

        /// <summary>
        /// 获取作业任务结果文件的物理路径
        /// </summary>
        /// <returns>The job task result path.</returns>
        /// <param name="taskPath">Task path.</param>
        public static string GetJobTaskResultPath(string taskPath)
        {
            return Path.Combine(taskPath, "result.txt");
        }

        /// <summary>
        /// 获取作业任务结果打包文件的磁盘物理路径
        /// </summary>
        /// <returns>The job task result path.</returns>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        /// <param name="taskId">Task identifier.</param>
        public static string GetJobTaskResultPackagePath(string jobName, string jobId, int taskId)
        {
            return Path.Combine(GetJobTaskRootPath(jobName, jobId, taskId), "result.zip");
        }

        /// <summary>
        /// 获取作业任务结果合并状态文件的物理路径
        /// </summary>
        /// <returns>The job task merge status path.</returns>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        public static string GetJobTaskMergeStatusPath(string jobName, string jobId)
        {
            return Path.Combine(GetJobRecordRootPath(jobName, jobId), "taskmerge.status");
        }

        /// <summary>
        /// 获取作业任务结果合并状态文件的物理路径
        /// </summary>
        /// <returns>The job task merge status path.</returns>
        /// <param name="jobRecordRootPath">Job record root path.</param>
        public static string GetJobTaskMergeStatusPath(string jobRecordRootPath)
        {
            return Path.Combine(jobRecordRootPath, "taskmerge.status");
        }

        /// <summary>
        /// 获取作业任务分割状态文件的物理路径
        /// </summary>
        /// <returns>The job task merge status path.</returns>
        /// <param name="jobRecordRootPath">Job record root path.</param>
        public static string GetJobSplitStatusPath(string jobRecordRootPath)
        {
            return Path.Combine(jobRecordRootPath, "taskcreate.status");
        }
        #endregion

        /// <summary>
        /// 获取文件操作锁的名称
        /// </summary>
        /// <returns>The file operate lock name.</returns>
        /// <param name="filePath">File path.</param>
        public static string GetFileOperateLockName(string filePath)
        {
            return "file:" + filePath;
        }
    }
}
