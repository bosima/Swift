using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
using Swift.Core.Log;

namespace Swift.Core
{
    /// <summary>
    /// 任务类
    /// </summary>
    public class JobTask
    {
        /// <summary>
        /// 任务Id
        /// </summary>
        public int Id
        {
            get;
            set;
        }

        /// <summary>
        /// 业务唯一Id
        /// </summary>
        /// <value>The unique identifier.</value>
        [JsonIgnore]
        public string BusinessId
        {
            get
            {
                if (Job == null)
                {
                    throw new InvalidOperationException("Job为null");
                }

                return FormatBusinessId(Job.Name, Job.Id, Id);
            }
        }

        /// <summary>
        /// 所属作业
        /// </summary>
        [JsonIgnore]
        public JobWrapper Job
        {
            get;
            set;
        }

        /// <summary>
        /// 任务需求
        /// </summary>
        [JsonIgnore]
        public string Requirement
        {
            get;
            set;
        }

        /// <summary>
        /// 任务状态
        /// </summary>
        public EnumTaskStatus Status
        {
            get;
            set;
        }

        /// <summary>
        /// 开始处理时间
        /// </summary>
        /// <value>The start time.</value>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// 完成时间
        /// </summary>
        /// <value>The finished time.</value>
        public DateTime FinishedTime { get; set; }

        /// <summary>
        /// 执行任务关连的计算机进程
        /// </summary>
        /// <value>The process.</value>
        [JsonIgnore]
        public SwiftProcess Process { get; set; }

        /// <summary>
        /// 执行超时分钟数
        /// </summary>
        /// <value>The timeout seconds.</value>
        public int ExecuteTimeout { get; set; }

        /// <summary>
        /// 任务结果
        /// </summary>
        [JsonIgnore]
        public string Result
        {
            get;
            set;
        }

        /// <summary>
        /// 当前任务所在路径
        /// </summary>
        [JsonIgnore]
        public string CurrentTaskPath
        {
            get
            {
                return SwiftConfiguration.GetJobTaskRootPath(Job.CurrentJobSpacePath, Id);
            }
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        public JobTask()
        {
        }

        /// <summary>
        /// Formats the business identifier.
        /// </summary>
        /// <returns>The business identifier.</returns>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        /// <param name="taskId">Task identifier.</param>
        public static string FormatBusinessId(string jobName, string jobId, int taskId)
        {
            return jobName + "_" + jobId + "_" + taskId;
        }

        /// <summary>
        /// 运行当前任务
        /// </summary>
        public void Run(CancellationToken cancellationToken = default(CancellationToken))
        {
            Job.RunTask(this, cancellationToken);
        }

        /// <summary>
        /// 监控运行当前任务，此时任务进程不受Swift控制，只能监控运行结果
        /// </summary>
        public void MointorRun(CancellationToken cancellationToken = default(CancellationToken))
        {
            Job.MointorRunTask(this, cancellationToken);
        }

        /// <summary>
        /// 根据任务配置文件创建任务实例
        /// </summary>
        /// <param name="physicalConfigPath"></param>
        /// <returns></returns>
        public static JobTask CreateInstance(string physicalConfigPath)
        {
            if (!File.Exists(physicalConfigPath))
            {
                throw new Exception(string.Format("任务配置文件不存在:{0}", physicalConfigPath));
            }

            var taskConfigJson = string.Empty;
            try
            {
                taskConfigJson = File.ReadAllText(physicalConfigPath, Encoding.UTF8);
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("读取任务配置文件异常:{0}", ex.Message));
            }

            if (string.IsNullOrWhiteSpace(taskConfigJson))
            {
                throw new Exception(string.Format("任务配置文件为空:{0}", physicalConfigPath));
            }

            var config = JsonConvert.DeserializeObject<JobTask>(taskConfigJson);

            return config;
        }

        /// <summary>
        /// 更新任务状态
        /// </summary>
        /// <param name="status"></param>
        public void UpdateTaskStatus(EnumTaskStatus status, CancellationToken cancellationToken = default(CancellationToken))
        {
            var result = Job.Cluster.ConfigCenter.TryUpdateTaskStatus(this, status, out int errCode, out JobBase latestJob, cancellationToken);
            LogWriter.Write(string.Format("更新任务状态结果:{0}", errCode));
            if (errCode == 0 || errCode == 2)
            {
                Status = status;
            }
        }

        #region 关连的计算机进程
        /// <summary>
        /// 获取进程Id
        /// </summary>
        /// <returns>The process identifier.</returns>
        public int GetProcessId(CancellationToken cancellationToken = default(CancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            int processId = -1;
            var processFilePath = SwiftConfiguration.GetSwiftProcessPath("ExecuteTask", BusinessId);
            if (File.Exists(processFilePath))
            {
                processId = int.Parse(File.ReadAllTextAsync(processFilePath, cancellationToken).Result);
            }

            return processId;
        }

        /// <summary>
        /// 创建进程文件
        /// </summary>
        public void CreateProcessFile()
        {
            var processDirPath = SwiftConfiguration.AllSwiftProcessRootPath;
            if (!Directory.Exists(processDirPath))
            {
                Directory.CreateDirectory(processDirPath);
            }

            // 在进程内部记录进程Id，方便跟踪Swift启动的进程
            var processFilePath = SwiftConfiguration.GetSwiftProcessPath("ExecuteTask", BusinessId);
            File.WriteAllText(processFilePath, Process.Id.ToString());
        }

        /// <summary>
        /// 移除进程文件
        /// </summary>
        public void RemoveProcessFile()
        {
            try
            {
                var processFilePath = SwiftConfiguration.GetSwiftProcessPath("ExecuteTask", BusinessId);
                File.Delete(processFilePath);
                LogWriter.Write("has remove task execute process file");
            }
            catch (Exception ex)
            {
                LogWriter.Write("remove task execute process file go exception", ex);
            }
        }

        /// <summary>
        /// 杀死关连的计算机进程
        /// </summary>
        public void KillProcess()
        {
            if (Process == null)
            {
                LogWriter.Write("进程实例不存在，认为进程未启动", LogLevel.Warn);
                return;
            }

            if (Process.IsExist)
            {
                LogWriter.Write("进程实例已关闭", LogLevel.Warn);
                RemoveProcessFile();
                return;
            }

            var hasExited = Process.HasExited;
            if (hasExited)
            {
                LogWriter.Write("进程已经退出");
                RemoveProcessFile();
                return;
            }

            try
            {
                Process.Kill();
                LogWriter.Write("任务关连的进程已执行Kill");

                Process.WaitForExit();
                RemoveProcessFile();
            }
            catch (Exception ex)
            {
                LogWriter.Write("Kill任务的进程时异常，请检查任务是否还在运行", ex);
            }
        }
        #endregion

        #region 任执行状态
        /// <summary>
        /// 获取任务执行状态
        /// </summary>
        /// <returns>The task execute status.</returns>
        public CommonResult GetTaskExecuteStatus()
        {
            // 读取任务创建状态
            string physicalPath = SwiftConfiguration.GetJobTaskExecuteStatusPath(Job.Name, Job.Id, Id);
            if (!File.Exists(physicalPath))
            {
                return new CommonResult()
                {
                    ErrCode = 1,
                    ErrMessage = "任务执行等待中..."
                };
            }

            var taskExecuteStatus = "";
            try
            {
                taskExecuteStatus = File.ReadAllText(physicalPath);
                LogWriter.Write(string.Format("任务执行状态文件内容：{0},{1}", physicalPath, taskExecuteStatus), LogLevel.Debug);
            }
            catch (Exception ex)
            {
                return new CommonResult()
                {
                    ErrCode = 2,
                    ErrMessage = "读取任务执行状态异常：" + ex.Message
                };
            }

            // 正在写入
            if (taskExecuteStatus == "1")
            {
                return new CommonResult()
                {
                    ErrCode = 3,
                    ErrMessage = "任务执行中..."
                };
            }

            // 任务处理出错
            if (taskExecuteStatus.StartsWith("-1", StringComparison.Ordinal))
            {
                return new CommonResult()
                {
                    ErrCode = 4,
                    ErrMessage = "任务执行出错：" + taskExecuteStatus
                };
            }

            return new CommonResult()
            {
                ErrCode = 0,
                ErrMessage = "任务执行完毕"
            };
        }
        #endregion

        /// <summary>
        /// 加载结果
        /// </summary>
        public void LoadResult()
        {
            var physicalResultPath = SwiftConfiguration.GetJobTaskResultPath(CurrentTaskPath);

            if (!File.Exists(physicalResultPath))
            {
                throw new Exception(string.Format("任务结果文件不存在:{0}", physicalResultPath));
            }

            var taskResult = string.Empty;
            try
            {
                taskResult = File.ReadAllText(physicalResultPath, Encoding.UTF8);
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("读取任务结果文件异常:{0}", ex.Message));
            }

            if (string.IsNullOrWhiteSpace(taskResult))
            {
                throw new Exception(string.Format("任务结果文件为空:{0}", physicalResultPath));
            }

            Result = taskResult;
        }

        /// <summary>
        /// 加载需求
        /// </summary>
        public void LoadRequirement()
        {
            var physicalRequirementPath = SwiftConfiguration.GetJobTaskRequirementPath(CurrentTaskPath);

            if (!File.Exists(physicalRequirementPath))
            {
                throw new Exception(string.Format("任务需求文件不存在:{0}", physicalRequirementPath));
            }

            var taskRequirementJson = string.Empty;
            try
            {
                taskRequirementJson = File.ReadAllText(physicalRequirementPath, Encoding.UTF8);
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("读取任务需求文件异常:{0}", ex.Message));
            }

            if (string.IsNullOrWhiteSpace(taskRequirementJson))
            {
                throw new Exception(string.Format("任务需求文件为空:{0}", physicalRequirementPath));
            }

            Requirement = taskRequirementJson;
        }

        /// <summary>
        /// 将任务配置写到文件
        /// </summary>
        public void WriteConfig(CancellationToken cancellationToken = default(CancellationToken))
        {
            EnsureJobTaskDirectory(cancellationToken);

            var taskJson = JsonConvert.SerializeObject(this);
            string taskJsonPath = SwiftConfiguration.GetJobTaskConfigPath(CurrentTaskPath);
            File.WriteAllTextAsync(taskJsonPath, taskJson, Encoding.UTF8, cancellationToken).Wait();
        }

        /// <summary>
        /// 确保作业任务目录存在
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        private void EnsureJobTaskDirectory(CancellationToken cancellationToken = default(CancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var taskPath = CurrentTaskPath;
            if (!Directory.Exists(taskPath))
            {
                Directory.CreateDirectory(taskPath);
            }
        }

        /// <summary>
        /// 将任务需求写到文件
        /// </summary>
        public void WriteRequirement()
        {
            var taskPath = CurrentTaskPath;
            if (!Directory.Exists(taskPath))
            {
                Directory.CreateDirectory(taskPath);
            }

            string taskRequirementPath = SwiftConfiguration.GetJobTaskRequirementPath(taskPath);
            File.WriteAllText(taskRequirementPath, Requirement, Encoding.UTF8);
        }

        /// <summary>
        /// 保存任务结果
        /// </summary>
        public void WriteResult()
        {
            var taskPath = CurrentTaskPath;
            if (!Directory.Exists(taskPath))
            {
                Directory.CreateDirectory(taskPath);
            }

            string taskRequirementPath = SwiftConfiguration.GetJobTaskResultPath(taskPath);
            File.WriteAllText(taskRequirementPath, Result, Encoding.UTF8);
        }

    }
}
