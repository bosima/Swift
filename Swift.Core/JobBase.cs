using Consul;
using Newtonsoft.Json;
using Swift.Core.Consul;
using Swift.Core.ExtensionException;
using Swift.Core.Log;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;

namespace Swift.Core
{
    /// <summary>
    /// 作业基类
    /// </summary>
    public abstract class JobBase
    {
        /// <summary>
        /// The job process dictionary.
        /// </summary>
        private readonly ConcurrentDictionary<string, SwiftProcess> _jobProcessDictionary = new ConcurrentDictionary<string, SwiftProcess>();

        /// <summary>
        /// 作业Id：用于区分不同的作业处理记录
        /// </summary>
        public string Id
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
                return FormatBusinessId(Name, Id);
            }
        }

        /// <summary>
        /// 作业名称：用于区分不同的作业
        /// </summary>
        public string Name
        {
            get;
            set;
        }

        /// <summary>
        /// 作业版本
        /// </summary>
        public string Version
        {
            get;
            set;
        }

        /// <summary>
        /// 文件名称
        /// </summary>
        public string FileName
        {
            get;
            set;
        }

        /// <summary>
        /// 可执行文件类型
        /// </summary>
        public string ExeType
        {
            get;
            set;
        }

        /// <summary>
        /// 作业类名称
        /// </summary>
        public string JobClassName
        {
            get;
            set;
        }

        /// <summary>
        /// Swift成员不可用的时间阈值，单位分钟，默认10。
        /// 如果已经分配任务的成员连续不可用超过此时间，则将此成员的任务重新分配给其它正常成员。
        /// </summary>
        /// <value>The re make task plan.</value>
        public int MemberUnavailableThreshold { get; set; }

        /// <summary>
        /// 单个任务执行超时时间，单位分钟，默认1440
        /// </summary>
        /// <value>The task execute timeout.</value>
        public int TaskExecuteTimeout { get; set; }

        /// <summary>
        /// 作业分割执行超时时间，单位分钟，默认120
        /// </summary>
        /// <value>The task execute timeout.</value>
        public int JobSplitTimeout { get; set; }

        /// <summary>
        /// 任务结果合并执行超时时间，单位分钟，默认120
        /// </summary>
        /// <value>The task execute timeout.</value>
        public int TaskResultCollectTimeout { get; set; }

        /// <summary>
        /// 当前作业实例的物理路径
        /// </summary>
        [JsonIgnore]
        public string CurrentJobSpacePath
        {
            get
            {
                return SwiftConfiguration.GetJobRecordRootPath(Name, Id);
            }
        }

        /// <summary>
        /// 当前作业所有文件的物理路径
        /// </summary>
        [JsonIgnore]
        public string CurrentJobRootPath
        {
            get
            {
                return SwiftConfiguration.GetJobRootPath(Name);
            }
        }

        /// <summary>
        /// Swift实例下全部作业根路径
        /// </summary>
        [JsonIgnore]
        public string JobRootPath
        {
            get
            {
                return SwiftConfiguration.AllJobRootPath;
            }
        }

        /// <summary>
        /// 作业状态
        /// </summary>
        public EnumJobRecordStatus Status
        {
            get;
            set;
        }

        /// <summary>
        /// 更改索引
        /// </summary>
        public ulong ModifyIndex
        {
            get;
            set;
        }

        /// <summary>
        /// 作业创建时间
        /// </summary>
        public DateTime CreateTime
        {
            get;
            set;
        }

        /// <summary>
        /// 作业开始时间
        /// </summary>
        public DateTime StartTime
        {
            get;
            set;
        }

        /// <summary>
        /// 作业完成时间
        /// </summary>
        public DateTime FinishedTime
        {
            get;
            set;
        }

        /// <summary>
        /// 作业分割开始时间
        /// </summary>
        public DateTime JobSplitStartTime
        {
            get;
            set;
        }

        /// <summary>
        /// 作业分割完成时间
        /// </summary>
        public DateTime JobSplitEndTime
        {
            get;
            set;
        }

        /// <summary>
        /// 任务合并开始时间
        /// </summary>
        public DateTime CollectTaskResultStartTime
        {
            get;
            set;
        }

        /// <summary>
        /// 任务合并结束时间
        /// </summary>
        public DateTime CollectTaskResultEndTime
        {
            get;
            set;
        }

        /// <summary>
        /// 任务计划
        /// </summary>
        public Dictionary<string, IEnumerable<JobTask>> TaskPlan
        {
            get;
            set;
        }

        /// <summary>
        /// 当前集群
        /// </summary>
        [JsonIgnore]
        public Cluster Cluster
        {
            get;
            set;
        }

        /// <summary>
        /// 从其它实例复制元数据
        /// </summary>
        /// <param name="job">Job.</param>
        public void CopyMetaFrom(JobBase job)
        {
            this.FileName = job.FileName;
            this.Id = job.Id;
            this.Name = job.Name;
            this.JobClassName = job.JobClassName;
            this.Status = job.Status;
            this.TaskPlan = job.TaskPlan;
            this.CreateTime = job.CreateTime;
            this.ModifyIndex = job.ModifyIndex;
            this.Version = job.Version;
            this.TaskExecuteTimeout = job.TaskExecuteTimeout;
            this.JobSplitTimeout = job.JobSplitTimeout;
            this.TaskResultCollectTimeout = job.TaskResultCollectTimeout;
            this.MemberUnavailableThreshold = job.MemberUnavailableThreshold;
        }

        /// <summary>
        /// 从配置文件创建实例
        /// </summary>
        /// <param name="jobConfig"></param>
        /// <returns></returns>
        public static JobWrapper CreateInstance(JobConfig jobConfig, Cluster cluster)
        {
            JobWrapper job = new JobWrapper
            {
                Id = DateTime.Now.ToString("yyyyMMddHHmmssfff"),
                Name = jobConfig.Name,
                JobClassName = jobConfig.JobClassName,
                FileName = jobConfig.FileName,
                ExeType = jobConfig.ExeType,
                Cluster = cluster,
                CreateTime = DateTime.Now,
                Version = jobConfig.Version,
                TaskExecuteTimeout = jobConfig.TaskExecuteTimeout,
                JobSplitTimeout = jobConfig.JobSplitTimeout,
                TaskResultCollectTimeout = jobConfig.TaskResultCollectTimeout,
                MemberUnavailableThreshold = jobConfig.MemberUnavailableThreshold,
            };
            return job;
        }

        /// <summary>
        /// 使用作业记录Json反序列化为作业记录
        /// </summary>
        /// <param name="jobJson"></param>
        /// <param name="cluster"></param>
        /// <returns></returns>
        public static JobWrapper Deserialize(string jobJson, Cluster cluster)
        {
            var jobWrapper = JsonConvert.DeserializeObject<JobWrapper>(jobJson);
            jobWrapper.Cluster = cluster;

            if (jobWrapper.TaskPlan != null && jobWrapper.TaskPlan.Count > 0)
            {
                foreach (var tasks in jobWrapper.TaskPlan.Values)
                {
                    foreach (var task in tasks)
                    {
                        task.Job = jobWrapper;
                    }
                }
            }

            return jobWrapper;
        }

        /// <summary>
        /// Formats the business identifier.
        /// </summary>
        /// <returns>The business identifier.</returns>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        public static string FormatBusinessId(string jobName, string jobId)
        {
            return jobName + "_" + jobId;
        }

        /// <summary>
        /// Relates the process.
        /// </summary>
        /// <param name="method">Method.</param>
        /// <param name="process">Process.</param>
        public void RelateProcess(string method, SwiftProcess process)
        {
            _jobProcessDictionary.TryAdd(method, process);
        }
        #region 制定作业计划
        /// <summary>
        /// 替换任务计划中离线的Worker
        /// </summary>
        public void ReplaceTaskPlanOfflineWorker(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (TaskPlan == null || TaskPlan.Count <= 0)
            {
                return;
            }

            var latestWorkers = Cluster.GetLatestWorkers(cancellationToken);
            var onlineWorkers = latestWorkers.Where(d => d.Status == 1).ToArray();
            bool hasWorkerStatusChanged = false;
            var taskMemberIds = TaskPlan.Keys.ToArray();

            foreach (var memberId in taskMemberIds)
            {
                var jobTasks = TaskPlan[memberId].ToArray();

                // 如果任务已经完成，工人剧本切换到管理员剧本，则这个任务不用重新分配
                if (memberId == Cluster.Manager.Id
                     && jobTasks.Count(d => d.Status == EnumTaskStatus.Completed
                        || d.Status == EnumTaskStatus.Synced) == jobTasks.Length)
                {
                    continue;
                }

                if (!onlineWorkers.Any(d => d.Id == memberId))
                {
                    if (Status == EnumJobRecordStatus.Canceling)
                    {
                        hasWorkerStatusChanged = SetTaskStatusToCanceldInCancelingJob(jobTasks);
                        LogWriter.Write(string.Format("作业取消中，将下线Worker的任务状态全部设为已取消：{0}", memberId), Log.LogLevel.Warn);
                        continue;
                    }

                    var offlineWorker = latestWorkers.FirstOrDefault(d => d.Id == memberId);
                    if (CheckIsNeedReplaceTaskWorker(offlineWorker))
                    {
                        LogWriter.Write("离线工人还有希望上线，暂不移交任务:" + memberId, Log.LogLevel.Debug);
                        continue;
                    }

                    LogWriter.Write("准备移交工人任务：" + memberId, Log.LogLevel.Debug);
                    LogWriter.Write("Worker任务：" + JsonConvert.SerializeObject(jobTasks), Log.LogLevel.Trace);

                    // 如果存在作业没有使用的新工人，则使用新工人；否则尽可能平均分配给现有的成员
                    hasWorkerStatusChanged = ReplcaeTaskWorker(onlineWorkers, taskMemberIds, memberId, jobTasks);
                }
            }

            if (hasWorkerStatusChanged)
            {
                // 保存作业计划，合并的时候需要
                WriteJobSpaceConfig(cancellationToken);

                UpdateJobTaskPlan(cancellationToken);
            }
        }

        /// <summary>
        /// 替换任务工人
        /// </summary>
        /// <returns><c>true</c>, if task worker was replcaed, <c>false</c> otherwise.</returns>
        /// <param name="onlineWorkers">在线工人数组</param>
        /// <param name="taskMemberIds">任务所有工人ID数组</param>
        /// <param name="currentMemberId">当前任务所属工人Id</param>
        /// <param name="jobTasks">要处理的任务数组</param>
        private bool ReplcaeTaskWorker(Member[] onlineWorkers, string[] taskMemberIds, string currentMemberId, JobTask[] jobTasks)
        {
            bool hasWorkerStatusChanged = false;

            var newWorker = onlineWorkers.FirstOrDefault(d => !taskMemberIds.Contains(d.Id));
            if (newWorker != null)
            {
                foreach (var jobTask in jobTasks)
                {
                    jobTask.StartTime = DateTime.MinValue;
                    jobTask.FinishedTime = DateTime.MinValue;
                    jobTask.Status = EnumTaskStatus.Pending;
                }

                TaskPlan.Remove(currentMemberId);
                TaskPlan.Add(newWorker.Id, jobTasks);
                hasWorkerStatusChanged = true;
                LogWriter.Write("替换任务的下线工人:" + currentMemberId + "->" + newWorker.Id, Log.LogLevel.Warn);
            }
            else
            {
                if (onlineWorkers.Length > 0)
                {
                    int i = 0;
                    foreach (var jobTask in jobTasks)
                    {
                        var onlineWorker = onlineWorkers[i];
                        jobTask.StartTime = DateTime.MinValue;
                        jobTask.FinishedTime = DateTime.MinValue;
                        jobTask.Status = EnumTaskStatus.Pending;
                        TaskPlan[onlineWorker.Id] = TaskPlan[onlineWorker.Id].Append(jobTask);

                        i++;
                        if (i >= onlineWorkers.Length)
                        {
                            i = 0;
                        }
                    }

                    TaskPlan.Remove(currentMemberId);
                    hasWorkerStatusChanged = true;
                    LogWriter.Write("转移下线工人的任务:" + currentMemberId, Log.LogLevel.Warn);
                }
            }

            return hasWorkerStatusChanged;
        }

        /// <summary>
        /// 设置取消中作业的任务状态为已取消
        /// </summary>
        /// <returns><c>true</c>, if task status to canceld in canceling job was set, <c>false</c> otherwise.</returns>
        /// <param name="jobTasks">Job tasks.</param>
        private static bool SetTaskStatusToCanceldInCancelingJob(JobTask[] jobTasks)
        {
            bool hasWorkerStatusChanged;
            foreach (var jobTask in jobTasks)
            {
                jobTask.Status = EnumTaskStatus.Canceled;
            }
            hasWorkerStatusChanged = true;

            return hasWorkerStatusChanged;
        }

        /// <summary>
        /// 检查是否需要替换任务工人
        /// </summary>
        /// <returns><c>true</c>, if is need replace task worker was checked, <c>false</c> otherwise.</returns>
        /// <param name="offlineWorker">Offline worker.</param>
        private bool CheckIsNeedReplaceTaskWorker(Member offlineWorker)
        {
            if (offlineWorker != null
                && DateTime.Now.Subtract(offlineWorker.OfflineTime.Value).TotalMinutes < MemberUnavailableThreshold)
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// 制定作业计划
        /// </summary>
        public void CreateProductionPlan(CancellationToken cancellationToken)
        {
            LogWriter.Write(string.Format("开始创建作业计划：{0},{1}", Name, Id));

            var onlineWorkers = Cluster.GetLatestWorkers(cancellationToken).Where(d => d.Status == 1).ToArray();
            if (onlineWorkers.Length <= 0)
            {
                LogWriter.Write("没有在线的工人，无法创建作业计划。");
                return;
            }

            // 更新作业状态为PlanMaking
            UpdateJobStatus(EnumJobRecordStatus.PlanMaking, cancellationToken);

            // 创建当前作业空间
            CreateJobSpace(cancellationToken);

            // 调用分割作业的方法
            CallJobSplitMethod(cancellationToken);

            // 阻塞检查作业分割进度
            bool isJobSplitOK = CheckJobSplitStatus(out int errorCode, out string errorMessage);
            LogWriter.Write(errorMessage);

            if (!isJobSplitOK)
            {
                UpdateJobStatus(EnumJobRecordStatus.PlanFailed, cancellationToken);
                LogWriter.Write(string.Format("作业分割失败。"));
            }
            else
            {
                var tasks = LoadTasksFromFile(cancellationToken).ToArray();

                // TODO:考虑每个工人现在的工作量，尽量分配给空闲的节点

                // 计算每个工人分配的任务数量
                var taskNumPerWorker = (int)Math.Ceiling(tasks.Length / (double)onlineWorkers.Length);

                // 将任务发给工人处理
                Dictionary<string, IEnumerable<JobTask>> workerTaskAssign = new Dictionary<string, IEnumerable<JobTask>>();
                for (int i = 0; i < onlineWorkers.Length; i++)
                {
                    var workTasks = tasks.Skip(i * taskNumPerWorker).Take(taskNumPerWorker);
                    workerTaskAssign.Add(onlineWorkers[i].Id, workTasks);
                }

                this.TaskPlan = workerTaskAssign;

                // 保存作业计划，合并的时候需要
                WriteJobSpaceConfig(cancellationToken);

                UpdateJobStatus(EnumJobRecordStatus.PlanMaked, cancellationToken);
                LogWriter.Write(string.Format("作业计划制定完毕。"));
            }
        }

        /// <summary>
        /// 调用作业分割方法，将调用具体作业实现的分割方法
        /// </summary>
        private void CallJobSplitMethod(CancellationToken cancellationToken = default(CancellationToken))
        {
            string taskCreateStatusPath = SwiftConfiguration.GetJobSplitStatusPath(CurrentJobSpacePath);

            var actions = new SwiftProcessEventActions
            {
                OutputAction = (s, e) =>
                 {
                     var msg = e.Data;
                     if (msg != null)
                     {
                         LogWriter.Write(string.Format("{0} output: {1}", s.BusinessId, msg));
                     }
                 },

                ErrorAction = (s, e) =>
                 {
                     var msg = e.Data;
                     if (msg != null)
                     {
                         try
                         {
                             File.WriteAllText(taskCreateStatusPath, "-1:" + (msg ?? string.Empty));
                         }
                         catch (Exception ex)
                         {
                             LogWriter.Write("write taskcreate.status go exception", ex);
                         }

                         LogWriter.Write(msg);
                     }
                 },

                ExitAction = (s, e) =>
                 {
                     try
                     {
                         LogWriter.Write("split job process exit:" + s.ExitCode);
                     }
                     catch (Exception ex)
                     {
                         LogWriter.Write("get split job process exit code go exception", ex);
                     }
                 },

                TimeoutAction = (s, e) =>
                {
                    try
                    {
                        File.WriteAllText(taskCreateStatusPath, "-1:task create timeout");
                    }
                    catch (Exception ex)
                    {
                        LogWriter.Write("write taskcreate.status with timeout go exception", ex);
                    }
                }
            };

            SwiftProcess process = new SwiftProcess("SplitJob", this, actions);
            process.SplitJob(cancellationToken);
        }

        /// <summary>
        /// 为当前作业创建作业空间
        /// </summary>
        public void CreateJobSpace(CancellationToken cancellationToken = default(CancellationToken))
        {
            ClearJobSpace(cancellationToken);

            CreateJobSpaceDirectory(cancellationToken);

            ExtractJobPackageToJobSpace(cancellationToken);

            WriteJobSpaceConfig(cancellationToken);
        }

        /// <summary>
        /// 解压作业包到作业空间
        /// </summary>
        private void ExtractJobPackageToJobSpace(CancellationToken cancellationToken = default(CancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            // 将程序包解压到当前作业的目录
            var pkgPath = SwiftConfiguration.GetJobPackagePath(Name, Version);
            var jobPkgLockName = SwiftConfiguration.GetFileOperateLockName(pkgPath);
            lock (string.Intern(jobPkgLockName))
            {
                ZipFile.ExtractToDirectory(pkgPath, CurrentJobSpacePath, true);
            }
        }

        /// <summary>
        /// 创建作业空间目录
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        private void CreateJobSpaceDirectory(CancellationToken cancellationToken = default(CancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            // 当前作业文件夹
            if (!Directory.Exists(CurrentJobSpacePath))
            {
                Directory.CreateDirectory(CurrentJobSpacePath);
            }
        }

        /// <summary>
        /// 清空作业空间
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        private void ClearJobSpace(CancellationToken cancellationToken = default(CancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            // 清除目录下的内容
            try
            {
                if (Directory.Exists(CurrentJobSpacePath))
                {
                    Directory.Delete(CurrentJobSpacePath, true);
                }
            }
            catch (DirectoryNotFoundException ex)
            {
                LogWriter.Write("作业空间还不存在，无需清空", ex, Log.LogLevel.Debug);
            }
        }

        /// <summary>
        /// 生成当前作业的任务，由具体的作业调用此方法
        /// </summary>
        public void GenerateTasks()
        {
            // 在进程内部记录进程Id，方便跟踪Swift启动的进程
            CreateProcessFile("SplitJob", Process.GetCurrentProcess().Id);

            Console.WriteLine("开始生成当前作业的任务");

            // 保存任务创建状态文件
            string taskCreateStatusFilePath = SwiftConfiguration.GetJobSplitStatusPath(CurrentJobSpacePath);
            File.WriteAllText(taskCreateStatusFilePath, "1");

            try
            {
                var tasks = Split();

                // 保存任务文件
                foreach (var task in tasks)
                {
                    task.ExecuteTimeout = TaskExecuteTimeout;
                    task.Job = new JobWrapper(this);
                    task.WriteConfig();
                    task.WriteRequirement();
                }

                File.WriteAllText(taskCreateStatusFilePath, "0");
                Console.WriteLine("GenerateTasks:OK");
            }
            catch (Exception ex)
            {
                File.WriteAllText(taskCreateStatusFilePath, "-1:" + ex.Message);
                Console.WriteLine("GenerateTasks:Error:" + ex.Message + ex.StackTrace);
            }
        }

        /// <summary>
        /// 将工作分成若干任务
        /// 任务中的需求可以是具体的需求，也可以是需求的某种指向（比如文件路径，建议数据量较大时使用）
        /// 当处理任务时，应使用相应的方法获取需求内容，然后进行处理
        /// </summary>
        /// <returns>任务数组</returns>
        public abstract JobTask[] Split();

        /// <summary>
        /// 此时进程已经脱离Swift控制，只能监控运行
        /// </summary>
        public void MointorRunJobSplit(CancellationToken cancellationToken = default(CancellationToken))
        {
            var isExecuteOK = BlockCheckJobSplitStatus(cancellationToken);
            if (isExecuteOK)
            {
                UpdateJobStatus(EnumJobRecordStatus.PlanMaked, cancellationToken);
                LogWriter.Write(string.Format("作业分割执行成功:{0}", BusinessId));
            }
            else
            {
                UpdateJobStatus(EnumJobRecordStatus.PlanFailed, cancellationToken);
                LogWriter.Write(string.Format("作业分割执行失败:{0}", BusinessId));
            }
        }

        /// <summary>
        /// 阻塞检查作业分割状态
        /// </summary>
        /// <returns></returns>
        private bool BlockCheckJobSplitStatus(CancellationToken cancellationToken = default(CancellationToken))
        {
            bool isOK = true;

            while (!CheckJobSplitStatus(out int errorCode, out string errorMessage))
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    // 如果取消了，则跳出循环，执行进程Kill
                    break;
                }

                LogWriter.Write(errorMessage, Log.LogLevel.Trace);

                // 明确出错
                if (errorCode == 3 || errorCode == 5)
                {
                    LogWriter.Write(errorMessage);
                    isOK = false;
                    break;
                }

                // 超时运行
                if (JobSplitTimeout * 60 < DateTime.Now.Subtract(JobSplitStartTime).TotalSeconds)
                {
                    LogWriter.Write(string.Format("monitor job split process timeout: {0}", BusinessId));
                    isOK = false;
                    break;
                }

                Thread.Sleep(3000);
            }

            // 进一步确保进程被关闭:超时、资源未释放可能导致进程刮起等情况
            KillJobSplitProcess();

            return isOK;
        }

        /// <summary>
        /// 检查作业分割状态
        /// </summary>
        /// <param name="errorCode">0任务已创建完毕，1作业目录未创建，2任务创建还未开始，3不能读取任务创建状态文件，4任务正在创建，5任务处理出错</param>
        /// <param name="errorMessage"></param> 
        /// <returns></returns>
        private bool CheckJobSplitStatus(out int errorCode, out string errorMessage)
        {
            var result = GetJobSplitStatus();

            errorCode = result.ErrCode;
            errorMessage = result.ErrMessage;

            if (errorCode == 0)
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// 获取作业分割状态
        /// </summary>
        /// <returns>The job split status.</returns>
        public CommonResult GetJobSplitStatus()
        {
            // 读取任务创建状态
            string physicalPath = SwiftConfiguration.GetJobSplitStatusPath(CurrentJobSpacePath);
            if (!File.Exists(physicalPath))
            {
                return new CommonResult()
                {
                    ErrCode = 2,
                    ErrMessage = "任务创建等待中..."
                };
            }

            var taskCreateStatus = "";
            try
            {
                taskCreateStatus = File.ReadAllText(physicalPath);
            }
            catch (Exception ex)
            {
                return new CommonResult()
                {
                    ErrCode = 3,
                    ErrMessage = "读取任务创建状态异常：" + ex.Message
                };
            }

            // 正在写入
            if (taskCreateStatus == "1")
            {
                return new CommonResult()
                {
                    ErrCode = 4,
                    ErrMessage = "任务创建中..."
                };
            }

            // 任务处理出错
            if (taskCreateStatus.StartsWith("-1", StringComparison.Ordinal))
            {
                return new CommonResult()
                {
                    ErrCode = 5,
                    ErrMessage = "创建任务出错：" + taskCreateStatus
                };
            }

            return new CommonResult()
            {
                ErrCode = 0,
                ErrMessage = "任务创建完毕。"
            };
        }
        #endregion

        #region 运行任务
        /// <summary>
        /// 运行任务
        /// </summary>
        /// <param name="task"></param>
        public void RunTask(JobTask task, CancellationToken cancellationToken = default(CancellationToken))
        {
            PullTaskRequirement(task, cancellationToken);

            StartRunTask(task, cancellationToken);

            CallTaskExecuteMethod(task, cancellationToken);

            cancellationToken.ThrowIfCancellationRequested();

            bool isExecuteOK = CheckTaskExecuteStatus(task, out int errorCode, out string errorMessage);
            LogWriter.Write(errorMessage);

            if (isExecuteOK)
            {
                task.UpdateTaskStatus(EnumTaskStatus.Completed, cancellationToken);
                LogWriter.Write(string.Format("任务执行成功:{0},{1},{2}", Name, Id, task.Id));
            }
            else
            {
                task.UpdateTaskStatus(EnumTaskStatus.Failed, cancellationToken);
                LogWriter.Write(string.Format("任务执行失败:{0},{1},{2}", Name, Id, task.Id));
            }
        }

        /// <summary>
        /// 此时进程已经脱离Swift控制，只能监控运行
        /// </summary>
        /// <param name="task">Task.</param>
        public void MointorRunTask(JobTask task, CancellationToken cancellationToken = default(CancellationToken))
        {
            var isExecuteOK = BlockCheckTaskExecuteStatus(task, cancellationToken);
            if (isExecuteOK)
            {
                task.UpdateTaskStatus(EnumTaskStatus.Completed, cancellationToken);
                LogWriter.Write(string.Format("任务执行成功:{0},{1},{2}", Name, Id, task.Id));
            }
            else
            {
                task.UpdateTaskStatus(EnumTaskStatus.Failed, cancellationToken);
                LogWriter.Write(string.Format("任务执行失败:{0},{1},{2}", Name, Id, task.Id));
            }
        }

        /// <summary>
        /// 拉取任务需求
        /// </summary>
        /// <param name="task"></param>
        private void PullTaskRequirement(JobTask task, CancellationToken cancellationToken = default(CancellationToken))
        {
            // 写task配置
            task.WriteConfig(cancellationToken);

            cancellationToken.ThrowIfCancellationRequested();

            // 下载需求文件
            var taskPath = SwiftConfiguration.GetJobTaskRootPath(task.Job.Name, task.Job.Id, task.Id);
            var taskRequirementPath = SwiftConfiguration.GetJobTaskRequirementPath(taskPath);
            if (!File.Exists(taskRequirementPath))
            {
                Cluster.CurrentMember.Download(Cluster.Manager, "download/task/requirement",
                    new Dictionary<string, string>
                    {
                        { "jobName", task.Job.Name },
                        { "jobId", task.Job.Id },
                        { "taskId", task.Id.ToString() },
                    },
                    cancellationToken
                );
            }
        }

        /// <summary>
        /// 开始处理任务
        /// </summary>
        /// <param name="task"></param>
        private void StartRunTask(JobTask task, CancellationToken cancellationToken = default(CancellationToken))
        {
            task.UpdateTaskStatus(EnumTaskStatus.Executing, cancellationToken);
            LogWriter.Write("已更新任务状态：" + task.BusinessId + "," + EnumTaskStatus.Executing, Log.LogLevel.Trace);

            UpdateJobStatus(EnumJobRecordStatus.TaskExecuting, cancellationToken);
            LogWriter.Write("已更新作业状态：" + task.Job.BusinessId + "," + EnumJobRecordStatus.TaskExecuting, Log.LogLevel.Trace);
        }

        /// <summary>
        /// 调用任务执行方法
        /// </summary>
        public void CallTaskExecuteMethod(JobTask task, CancellationToken cancellationToken = default(CancellationToken))
        {
            // 当前任务文件夹
            var currentTaskPath = SwiftConfiguration.GetJobTaskRootPath(CurrentJobSpacePath, task.Id);
            var taskExecuteStatusPath = SwiftConfiguration.GetJobTaskExecuteStatusPath(currentTaskPath);

            var actions = new SwiftProcessEventActions
            {
                OutputAction = (s, e) =>
                {
                    var msg = e.Data;
                    if (msg != null)
                    {
                        LogWriter.Write(string.Format("{0} output: {1}", s.BusinessId, msg));
                    }
                },

                ErrorAction = (s, e) =>
                {
                    var msg = e.Data;
                    if (msg != null)
                    {
                        try
                        {
                            File.WriteAllText(taskExecuteStatusPath, "-1:" + msg);
                        }
                        catch (Exception ex)
                        {
                            LogWriter.Write("write taskexecute.status with error go exception", ex);
                        }

                        LogWriter.Write(msg);
                    }
                },

                ExitAction = (s, e) =>
                {
                    try
                    {
                        LogWriter.Write("execute task process exit:" + s.ExitCode);
                    }
                    catch (Exception ex)
                    {
                        LogWriter.Write("get execute task process exit code go exception", ex);
                    }

                    task.RemoveProcessFile();
                },

                TimeoutAction = (s, e) =>
                {
                    try
                    {
                        File.WriteAllText(taskExecuteStatusPath, "-1:task execute timeout");
                    }
                    catch (Exception ex)
                    {
                        LogWriter.Write("write taskexecute.status with timeout go exception", ex);
                    }
                }
            };

            SwiftProcess process = new SwiftProcess("ExecuteTask", task, actions);
            process.ExecuteTask(cancellationToken);
        }

        /// <summary>
        /// 执行任务，由具体的作业调用此方法
        /// </summary>
        /// <param name="task"></param>
        public void PerformTask(JobTask task)
        {
            // 在进程内部记录进程Id，方便跟踪Swift启动的进程
            var process = new SwiftProcess("ExecuteTask", task, Process.GetCurrentProcess());
            task.CreateProcessFile();

            // 保存任务创建状态文件
            string taskExecuteStatusFilePath = SwiftConfiguration.GetJobTaskExecuteStatusPath(CurrentJobSpacePath, task.Id);
            File.WriteAllText(taskExecuteStatusFilePath, "1");

            try
            {
                var result = ExecuteTask(task);
                task.Result = result;
                task.WriteResult();

                File.WriteAllText(taskExecuteStatusFilePath, "0");
                Console.WriteLine("PerformTask:OK");
            }
            catch (System.Exception ex)
            {
                File.WriteAllText(taskExecuteStatusFilePath, "-1:" + ex.Message);
                Console.WriteLine("PerformTask:Error:" + ex.Message);
            }
        }

        /// <summary>
        /// 根据作业的具体方法执行任务，由具体的作业实现
        /// </summary>
        /// <returns>
        /// 任务处理结果:
        /// 处理结果可以是真正的处理结果，也可以是处理结果的某种指向（比如文件路径，建议数据量较大时使用）。
        /// 当合并时，应使用对应的方法获取任务处理结果，然后进行合并。
        /// </returns>
        public abstract string ExecuteTask(JobTask task);

        /// <summary>
        /// 阻塞检查任务执行状态，在进程脱离Swift控制情况下使用
        /// </summary>
        /// <param name="task"></param>
        /// <returns></returns>
        private bool BlockCheckTaskExecuteStatus(JobTask task, CancellationToken cancellationToken = default(CancellationToken))
        {
            bool isOK = true;

            while (!CheckTaskExecuteStatus(task, out int errorCode, out string errorMessage))
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    // 如果取消了，则跳出循环，执行进程Kill
                    break;
                }

                LogWriter.Write(errorMessage, Log.LogLevel.Trace);

                // 明确出错
                if (errorCode == 4)
                {
                    LogWriter.Write(errorMessage);
                    isOK = false;
                    break;
                }

                // 超时运行
                if (task.ExecuteTimeout * 60 < DateTime.Now.Subtract(task.StartTime).TotalSeconds)
                {
                    LogWriter.Write(string.Format("monitor task process timeout: {0}", task.BusinessId));
                    isOK = false;
                    break;
                }

                Thread.Sleep(3000);
            }

            // 进一步确保进程被关闭:超时、资源未释放可能导致进程刮起等情况
            task.KillProcess();

            return isOK;
        }

        /// <summary>
        /// 检查任务执行状态
        /// </summary>
        /// <param name="task"></param>
        /// <param name="errorCode">执行状态：0执行完毕，1执行等待中，2读取执行状态失败，3正在执行，4执行出错</param>
        /// <param name="errorMessage"></param>
        /// <returns></returns>
        private bool CheckTaskExecuteStatus(JobTask task, out int errorCode, out string errorMessage)
        {
            var result = task.GetTaskExecuteStatus();

            errorCode = result.ErrCode;
            errorMessage = result.ErrMessage;

            if (errorCode == 0)
            {
                return true;
            }

            return false;
        }
        #endregion

        #region 任务结果处理
        /// <summary>
        /// 同步任务结果
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        public void SyncTaskResult(CancellationToken cancellationToken = default(CancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var taskPlan = TaskPlan;

            if (taskPlan != null && taskPlan.Count > 0)
            {
                var executeCompleteTasks = taskPlan.SelectMany(d => d.Value
                .Where(t => t.Status == EnumTaskStatus.Completed
                || t.Status == EnumTaskStatus.SyncFailed));

                foreach (var task in executeCompleteTasks)
                {
                    bool pullResult = false;
                    try
                    {
                        PullTaskResult(task, cancellationToken);
                        pullResult = true;
                    }
                    catch (System.Exception ex)
                    {
                        LogWriter.Write(string.Format("同步任务结果异常:{0}", ex.Message), ex);
                    }

                    if (pullResult)
                    {
                        task.UpdateTaskStatus(EnumTaskStatus.Synced, cancellationToken);
                    }
                    else
                    {
                        task.UpdateTaskStatus(EnumTaskStatus.SyncFailed, cancellationToken);
                    }
                }
            }
        }

        /// <summary>
        /// 拉取任务结果
        /// </summary>
        /// <param name="task"></param>
        private void PullTaskResult(JobTask task, CancellationToken cancellationToken = default(CancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            // 任务所属成员
            var memberId = TaskPlan.Where(d => d.Value.Any(t => t.Id == task.Id)).Select(d => d.Key).FirstOrDefault();
            var member = Cluster.Members.FirstOrDefault(d => d.Id == memberId);
            if (member == null)
            {
                throw new MemberNotFoundException(string.Format("成员[{0}]不存在，无法拉取任务结果。", memberId));
            }

            // worker变为管理员
            if (member.Id == Cluster.CurrentMember.Id)
            {
                throw new MemberNotFoundException(string.Format("成员[{0}]自己完成的任务，不用拉取。", memberId));
            }

            // 下载任务结果
            Cluster.CurrentMember.Download(member, "download/task/result",
                new Dictionary<string, string>
                {
                    { "jobName", task.Job.Name },
                    { "jobId", task.Job.Id },
                    { "taskId", task.Id.ToString() },
                },
                cancellationToken
            );
        }

        /// <summary>
        /// 检查任务运行状态
        /// </summary>
        public void CheckTaskRunStatus(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (TaskPlan == null)
            {
                LogWriter.Write(string.Format("任务计划为空:{0},{1}", Name, Id));
                return;
            }
            var taskCount = TaskPlan.SelectMany(d => d.Value).Count();

            var executeCompleteTaskCount = TaskPlan.SelectMany(d => d.Value.Where(t =>
             t.Status == EnumTaskStatus.Completed
             || t.Status == EnumTaskStatus.Synced
             || t.Status == EnumTaskStatus.SyncFailed)).Count();

            var syncedTaskCount = TaskPlan.SelectMany(d => d.Value.Where(t => t.Status == EnumTaskStatus.Synced)).Count();
            var canceledTaskCount = TaskPlan.SelectMany(d => d.Value.Where(t => t.Status == EnumTaskStatus.Canceled)).Count();

            if (taskCount == syncedTaskCount)
            {
                UpdateJobStatus(EnumJobRecordStatus.TaskSynced, cancellationToken);
            }
            else if (taskCount == executeCompleteTaskCount)
            {
                UpdateJobStatus(EnumJobRecordStatus.TaskCompleted, cancellationToken);
            }
            else if (taskCount == canceledTaskCount)
            {
                UpdateJobStatus(EnumJobRecordStatus.Canceled, cancellationToken);
            }
        }

        /// <summary>
        /// 合并任务处理结果
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        public void MergeTaskResult(CancellationToken cancellationToken = default(CancellationToken))
        {
            // 更新作业状态为TaskMerging
            UpdateJobStatus(EnumJobRecordStatus.TaskMerging, cancellationToken);

            // 保存任务合并状态文件
            string taskCreateStatusFilePath = SwiftConfiguration.GetJobTaskMergeStatusPath(CurrentJobSpacePath);
            File.WriteAllTextAsync(taskCreateStatusFilePath, "1", cancellationToken).Wait();

            CallCollectTaskResultMethod(cancellationToken);

            // 阻塞并检查任务结果合并进度
            bool isMergeOK = CheckCollectTaskResultStatus(out int errorCode, out string errorMessage);
            LogWriter.Write(errorMessage);

            if (isMergeOK)
            {
                UpdateJobStatus(EnumJobRecordStatus.TaskMerged, cancellationToken);
                LogWriter.Write(string.Format("任务合并成功:{0},{1}", Name, Id));
            }
            else
            {
                UpdateJobStatus(EnumJobRecordStatus.TaskMergeFailed, cancellationToken);
                LogWriter.Write(string.Format("任务合并失败:{0},{1}", Name, Id));
            }
        }

        /// <summary>
        /// 合并所有任务结果
        /// </summary>
        public void CollectTaskResults()
        {
            var taskMergeStatusPath = SwiftConfiguration.GetJobTaskMergeStatusPath(CurrentJobSpacePath);
            string jobResultPath = SwiftConfiguration.GetJobResultPath(CurrentJobSpacePath);
            string jobZipResultPath = SwiftConfiguration.GetJobResultPackagePath(CurrentJobSpacePath);

            // 在进程内部记录进程Id，方便跟踪Swift启动的进程
            CreateProcessFile("CollectTaskResult", Process.GetCurrentProcess().Id);

            try
            {
                var tasks = TaskPlan.SelectMany(d => d.Value);
                foreach (var task in tasks)
                {
                    task.LoadResult();
                }
                LogWriter.Write("已经准备好要合并的任务");

                string result = Collect(tasks);
                LogWriter.Write("任务结果合并已经计算完毕");

                File.WriteAllText(jobResultPath, result);
                LogWriter.Write("已经将任务结果写入文件");

                using (var zip = ZipFile.Open(jobZipResultPath, ZipArchiveMode.Create))
                {
                    zip.CreateEntryFromFile(jobResultPath, "result.txt");
                }
                LogWriter.Write("已经打包作业结果");

                File.WriteAllText(taskMergeStatusPath, "0");
                Console.WriteLine("CollectTaskResults:OK");
            }
            catch (Exception ex)
            {
                File.WriteAllText(taskMergeStatusPath, "-1:" + ex.Message);
                Console.WriteLine("CollectTaskResults:Error:" + ex.Message + ex.StackTrace);
            }
        }

        /// <summary>
        /// 此时进程已经脱离Swift控制，只能监控运行
        /// </summary>
        public void MointorRunCollectTaskResult(CancellationToken cancellationToken = default(CancellationToken))
        {
            var isExecuteOK = BlockCheckCollectTaskResultStatus(cancellationToken);
            if (isExecuteOK)
            {
                UpdateJobStatus(EnumJobRecordStatus.TaskMerged, cancellationToken);
                LogWriter.Write(string.Format("任务合并执行成功:{0}", BusinessId));
            }
            else
            {
                UpdateJobStatus(EnumJobRecordStatus.TaskMergeFailed, cancellationToken);
                LogWriter.Write(string.Format("任务合并执行失败:{0}", BusinessId));
            }
        }

        /// <summary>
        /// 阻塞检查任务结果合并状态
        /// </summary>
        /// <returns></returns>
        private bool BlockCheckCollectTaskResultStatus(CancellationToken cancellationToken = default(CancellationToken))
        {
            bool isOK = true;

            while (!CheckCollectTaskResultStatus(out int errorCode, out string errorMessage))
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                LogWriter.Write(errorMessage, Log.LogLevel.Trace);

                // 明确出错
                if (errorCode == 4 || errorCode == 2)
                {
                    LogWriter.Write(errorMessage);
                    isOK = false;
                    break;
                }

                // 超时运行
                if (TaskResultCollectTimeout * 60 < DateTime.Now.Subtract(CollectTaskResultStartTime).TotalSeconds)
                {
                    LogWriter.Write(string.Format("monitor collect task result process timeout: {0}", this.BusinessId));
                    isOK = false;
                    break;
                }

                Thread.Sleep(3000);
            }

            KillCollectTaskResultProcess();

            return isOK;
        }

        /// <summary>
        /// 检查任务结果合并状态
        /// </summary>
        /// <param name="errorCode">1尚未创建合并状态，2不能读取任务合并状态，3任务合并中，4任务合并出错</param>
        /// <returns></returns>
        private bool CheckCollectTaskResultStatus(out int errorCode, out string errorMessage)
        {
            var result = GetCollectTaskResultStatus();

            errorCode = result.ErrCode;
            errorMessage = result.ErrMessage;

            if (errorCode == 0)
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// 获取任务结果合并状态
        /// </summary>
        /// <returns>The collect task result status.</returns>
        public CommonResult GetCollectTaskResultStatus()
        {
            // 读取任务合并状态
            string physicalPath = SwiftConfiguration.GetJobTaskMergeStatusPath(CurrentJobSpacePath);
            if (!File.Exists(physicalPath))
            {
                return new CommonResult()
                {
                    ErrCode = 1,
                    ErrMessage = "任务合并等待中...",
                };
            }

            var taskMergeStatus = "";
            try
            {
                taskMergeStatus = File.ReadAllText(physicalPath);
            }
            catch (Exception ex)
            {
                return new CommonResult()
                {
                    ErrCode = 2,
                    ErrMessage = "读取任务合并状态异常：" + ex.Message,
                };
            }

            // 正在合并
            if (taskMergeStatus == "1")
            {
                return new CommonResult()
                {
                    ErrCode = 3,
                    ErrMessage = "任务合并中...",
                };
            }

            // 任务处理出错
            if (taskMergeStatus.StartsWith("-1", StringComparison.Ordinal))
            {
                return new CommonResult()
                {
                    ErrCode = 4,
                    ErrMessage = "创建合并出错：" + taskMergeStatus
                };
            }

            return new CommonResult()
            {
                ErrCode = 0,
                ErrMessage = "任务合并完毕。"
            };
        }

        /// <summary>
        /// 调用任务合并方法，由Manager调用此方法
        /// </summary>
        public void CallCollectTaskResultMethod(CancellationToken cancellationToken = default(CancellationToken))
        {
            var taskMergeStatusPath = SwiftConfiguration.GetJobTaskMergeStatusPath(CurrentJobSpacePath);

            var actions = new SwiftProcessEventActions
            {
                OutputAction = (s, e) =>
                {
                    var msg = e.Data;
                    if (msg != null)
                    {
                        LogWriter.Write(string.Format("{0} output: {1}", s.BusinessId, msg));
                    }
                },

                ErrorAction = (s, e) =>
                {
                    var msg = e.Data;
                    if (msg != null)
                    {
                        try
                        {
                            File.WriteAllText(taskMergeStatusPath, "-1:" + msg);
                        }
                        catch (Exception ex)
                        {
                            LogWriter.Write("write taskmerge.status with error go exception", ex);
                        }

                        LogWriter.Write(msg);
                    }
                },

                ExitAction = (s, e) =>
                {
                    try
                    {
                        LogWriter.Write("merge task process exit:" + s.ExitCode);
                    }
                    catch (Exception ex)
                    {
                        LogWriter.Write("get merge task process exit code go exception", ex);
                    }
                },
                TimeoutAction = (s, e) =>
                {
                    try
                    {
                        File.WriteAllText(taskMergeStatusPath, "-1:task merge timeout");
                    }
                    catch (Exception ex)
                    {
                        LogWriter.Write("write taskmerge.status with timeout go exception", ex);
                    }
                }
            };

            SwiftProcess process = new SwiftProcess("CollectTaskResult", this, actions);
            process.CollectTaskResult(cancellationToken);
        }

        /// <summary>
        /// 汇集各个任务的处理结果
        /// 如果结果数据量很大，建议将结果保存到某个地方，这里只返回结果的索引
        /// </summary>
        /// <returns>作业处理结果</returns>
        public abstract string Collect(IEnumerable<JobTask> tasks);
        #endregion

        #region 作业配置处理
        /// <summary>
        /// 更新作业状态
        /// </summary>
        /// <param name="status"></param>
        public void UpdateJobStatus(EnumJobRecordStatus status, CancellationToken cancellationToken = default(CancellationToken))
        {
            Cluster.ConfigCenter.TryUpdateJobStatus(this, status, out int errCode, out JobBase latestJob, cancellationToken);
            LogWriter.Write(string.Format("更新作业记录状态结果:{0}", errCode));

            if (errCode == 0 || errCode == 2)
            {
                this.Status = status;
            }
        }

        /// <summary>
        /// 更新作业任务计划
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        private void UpdateJobTaskPlan(CancellationToken cancellationToken = default(CancellationToken))
        {
            var ccJobRecord = Cluster.ConfigCenter.UpdateJobTaskPlan(this, cancellationToken);
            LogWriter.Write(string.Format("更新作业记录状态结果:{0}", ccJobRecord != null));
            if (ccJobRecord != null)
            {
                this.Status = ccJobRecord.Status;
            }
        }

        /// <summary>
        /// 加载作业的任务
        /// </summary>
        public List<JobTask> LoadTasksFromFile(CancellationToken cancellationToken = default(CancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var taskList = new List<JobTask>();

            var taskPath = SwiftConfiguration.GetJobAllTaskRootPath(CurrentJobSpacePath);

            // 读取任务
            if (!Directory.Exists(taskPath))
            {
                return taskList;
            }

            var taskDirectories = Directory.GetDirectories(taskPath);

            if (taskDirectories != null || taskDirectories.Length > 0)
            {
                foreach (var taskDir in taskDirectories)
                {
                    var taskJsonPath = SwiftConfiguration.GetJobTaskConfigPath(taskDir);
                    var taskJson = File.ReadAllTextAsync(taskJsonPath, cancellationToken).Result;
                    var task = JsonConvert.DeserializeObject<JobTask>(taskJson);
                    task.Job = (JobWrapper)this;

                    taskList.Add(task);
                }
            }

            return taskList;
        }

        /// <summary>
        /// 写作业空间配置
        /// </summary>
        private void WriteJobSpaceConfig(CancellationToken cancellationToken = default(CancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            // 保存当前作业配置
            var jobConfigJson = JsonConvert.SerializeObject(this);
            string currentJobConfigFilePath = SwiftConfiguration.GetJobRecordConfigPath(CurrentJobSpacePath);
            File.WriteAllTextAsync(currentJobConfigFilePath, jobConfigJson, cancellationToken).Wait();
        }
        #endregion

        #region 进程处理
        /// <summary>
        /// 创建进程文件
        /// </summary>
        private void CreateProcessFile(string method, int processId)
        {
            var processDirPath = SwiftConfiguration.AllSwiftProcessRootPath;
            if (!Directory.Exists(processDirPath))
            {
                Directory.CreateDirectory(processDirPath);
            }

            var processFilePath = SwiftConfiguration.GetSwiftProcessPath(method, BusinessId);
            File.WriteAllText(processFilePath, processId.ToString());
        }

        /// <summary>
        /// Hases the related process.
        /// </summary>
        /// <returns><c>true</c>, if related process was hased, <c>false</c> otherwise.</returns>
        public bool HasRelatedProcess
        {
            get
            {
                if (_jobProcessDictionary.Count > 0)
                {
                    return true;
                }

                return false;
            }
        }

        /// <summary>
        /// 杀死所有相关进程
        /// </summary>
        public void KillRelatedProcess()
        {
            KillJobSplitProcess();
            KillCollectTaskResultProcess();
        }

        /// <summary>
        /// 杀死作业分割进程
        /// </summary>
        public void KillJobSplitProcess()
        {
            if (_jobProcessDictionary.TryGetValue("SplitJob", out SwiftProcess process))
            {
                LogWriter.Write(string.Format("尝试杀死进程：{0},{1}", BusinessId, "SplitJob"));
                KillProcess(process);

                _jobProcessDictionary.TryRemove("SplitJob", out process);
            }
        }

        /// <summary>
        /// 杀死作业任务合并进程
        /// </summary>
        public void KillCollectTaskResultProcess()
        {
            if (_jobProcessDictionary.TryGetValue("CollectTaskResult", out SwiftProcess process))
            {
                LogWriter.Write(string.Format("尝试杀死进程：{0},{1}", BusinessId, "CollectTaskResult"));
                KillProcess(process);

                _jobProcessDictionary.TryRemove("CollectTaskResult", out process);
            }
        }

        /// <summary>
        /// 杀死关连的计算机进程
        /// </summary>
        private void KillProcess(SwiftProcess process)
        {
            if (process == null)
            {
                LogWriter.Write("进程实例不存在，认为进程未启动", Log.LogLevel.Warn);
                return;
            }

            if (process.IsExist)
            {
                LogWriter.Write("进程实例已关闭", Log.LogLevel.Warn);
                RemoveProcessFile(process);
                return;
            }

            var hasExited = process.HasExited;
            if (hasExited)
            {
                LogWriter.Write("进程已经退出");
                RemoveProcessFile(process);
                return;
            }

            try
            {
                process.Kill();
                LogWriter.Write("进程已执行Kill");

                process.WaitForExit();
                RemoveProcessFile(process);
            }
            catch (Exception ex)
            {
                LogWriter.Write("Kill任务的进程时异常，请检查任务是否还在运行", ex);
            }
        }

        /// <summary>
        /// 移除进程文件
        /// </summary>
        private void RemoveProcessFile(SwiftProcess process)
        {
            try
            {
                var processFilePath = process.IdFilePath;
                if (string.IsNullOrWhiteSpace(processFilePath))
                {
                    LogWriter.Write("remove process file find it not executed: " + processFilePath);
                    return;
                }

                File.Delete(processFilePath);
                LogWriter.Write("has remove process file");
            }
            catch (Exception ex)
            {
                LogWriter.Write("remove process file go exception", ex);
            }
        }

        /// <summary>
        /// 获取进程Id
        /// </summary>
        /// <returns>The process identifier.</returns>
        public int GetProcessId(string method)
        {
            int processId = -1;
            var processFilePath = SwiftConfiguration.GetSwiftProcessPath(method, BusinessId);
            if (File.Exists(processFilePath))
            {
                processId = int.Parse(File.ReadAllText(processFilePath));
            }

            return processId;
        }
        #endregion
    }
}
