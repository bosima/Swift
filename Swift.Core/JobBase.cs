using Consul;
using Newtonsoft.Json;
using Swift.Core.Consul;
using Swift.Core.ExtensionException;
using Swift.Core.Log;
using System;
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
        /// 作业Id：用于区分不同的作业处理记录
        /// </summary>
        public string Id
        {
            get;
            set;
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
        /// 文件名称
        /// </summary>
        public string FileName
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
        /// 当前作业空间的物理路径
        /// </summary>
        [JsonIgnore]
        public string CurrentJobSpacePath
        {
            get
            {
                return Path.Combine(CurrentJobRootPath, Id);

            }
        }

        /// <summary>
        /// 当前作业路径
        /// </summary>
        [JsonIgnore]
        public string CurrentJobRootPath
        {
            get
            {
                return Path.Combine(JobRootPath, Name);
            }
        }

        /// <summary>
        /// 作业根路径
        /// </summary>
        [JsonIgnore]
        public string JobRootPath
        {
            get
            {
                var currentDirectory = Environment.CurrentDirectory;
                if (currentDirectory.IndexOf("Jobs") >= 0)
                {
                    return currentDirectory.Substring(0, currentDirectory.IndexOf("Jobs") + 4);
                }
                else
                {
                    return Path.Combine(currentDirectory, "Jobs");
                }
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
        /// 作业文件的字节数组
        /// </summary>
        public byte[] FileBytes
        {
            get;
            set;
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="id"></param>
        /// <param name="name"></param>
        public JobBase()
        {
        }

        /// <summary>
        /// 从配置文件创建实例
        /// </summary>
        /// <param name="jobConfig"></param>
        /// <returns></returns>
        public static JobWrapper CreateInstance(JobConfig jobConfig, Cluster cluster)
        {
            JobWrapper job = new JobWrapper();
            job.Id = DateTime.Now.ToString("yyyyMMddHHmmssfff");
            job.Name = jobConfig.Name;
            job.JobClassName = jobConfig.JobClassName;
            job.FileName = jobConfig.FileName;
            job.Cluster = cluster;
            job.CreateTime = DateTime.Now;
            return job;
        }

        /// <summary>
        /// 从配置文件路径创建实例
        /// </summary>
        /// <param name="physicalConfigPath"></param>
        /// <param name="cluster"></param>
        /// <returns></returns>
        public static JobWrapper CreateInstance(string physicalConfigPath, Cluster cluster)
        {
            JobConfig config = new JobConfig(physicalConfigPath);
            return CreateInstance(config, cluster);
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
        /// 从其他作业类更新
        /// </summary>
        /// <param name="job"></param>
        public void UpdateFrom(JobBase job)
        {
            Id = job.Id;
            Name = job.Name;
            FileName = job.FileName;
            JobClassName = job.JobClassName;
            Status = job.Status;
            ModifyIndex = job.ModifyIndex;
            CreateTime = job.CreateTime;
            TaskPlan = job.TaskPlan;
            Cluster = job.Cluster;
            FileBytes = job.FileBytes;
        }

        #region 制定作业计划
        /// <summary>
        /// 创建生产计划
        /// </summary>
        public void CreateProductionPlan()
        {
            LogWriter.Write(string.Format("开始创建作业计划：{0},{1}", Name, Id));

            // 更新作业状态为PlanMaking
            UpdateJobStatus(EnumJobRecordStatus.PlanMaking);

            // 创建当前作业空间
            CreateJobSpace();

            // 调用分割作业的方法
            CallJobSplitMethod();

            // 阻塞检查作业分割进度
            bool isJobSplitOK = BlockCheckJobSplitStatus();

            if (!isJobSplitOK)
            {
                UpdateJobStatus(EnumJobRecordStatus.PlanFailed);
                LogWriter.Write(string.Format("作业计划制定失败。"));
            }
            else
            {
                var tasks = LoadTasksFromFile().ToArray();

                // 计算每个工人分配的任务数量
                var taskNumPerWorker = (int)Math.Ceiling(tasks.Length / (double)Cluster.Workers.Length);

                // 将任务发给工人处理
                Dictionary<string, IEnumerable<JobTask>> workerTaskAssign = new Dictionary<string, IEnumerable<JobTask>>();
                for (int i = 0; i < Cluster.Workers.Length; i++)
                {
                    var workTasks = tasks.Skip(i * taskNumPerWorker).Take(taskNumPerWorker);
                    workerTaskAssign.Add(Cluster.Workers[i].Id, workTasks);
                }

                this.TaskPlan = workerTaskAssign;

                WriteJobSpaceConfig();

                UpdateJobStatus(EnumJobRecordStatus.PlanMaked);
                LogWriter.Write(string.Format("作业计划制定完毕。"));
            }
        }

        /// <summary>
        /// 调用作业分割方法，将调用具体作业实现的分割方法
        /// </summary>
        private void CallJobSplitMethod()
        {
            try
            {
                Process p = new Process();

                // 当前作业文件夹
                var currentJobStartPath = Path.Combine(CurrentJobSpacePath, FileName);

                string m_cmdLine = string.Format(" -d");
                p.StartInfo.WorkingDirectory = CurrentJobSpacePath;
                p.StartInfo.FileName = currentJobStartPath;
                p.StartInfo.Arguments = currentJobStartPath + m_cmdLine;
                p.StartInfo.UseShellExecute = false;
                p.StartInfo.RedirectStandardInput = true;
                p.StartInfo.RedirectStandardOutput = true;
                p.StartInfo.RedirectStandardError = true;
                p.StartInfo.CreateNoWindow = true;
                p.EnableRaisingEvents = true;

                p.OutputDataReceived += (s, e) =>
                {
                    if (e.Data != null && e.Data.StartsWith("GenerateTasks"))
                    {
                        if (e.Data.StartsWith("GenerateTasks:OK"))
                        {
                            string physicalPath = Path.Combine(CurrentJobSpacePath, "taskcreate.status");
                            File.WriteAllText(physicalPath, "0");
                        }

                        if (e.Data.StartsWith("GenerateTasks:Error"))
                        {
                            string physicalPath = Path.Combine(CurrentJobSpacePath, "taskcreate.status");
                            File.WriteAllText(physicalPath, "-1:" + e.Data);
                        }

                        if (!p.HasExited)
                        {
                            p.Close();
                            p.Dispose();
                        }
                    }
                };

                p.ErrorDataReceived += (s, e) =>
                {
                    if (e.Data != null)
                    {
                        string physicalPath = Path.Combine(CurrentJobSpacePath, "taskcreate.status");
                        File.WriteAllText(physicalPath, "-1:" + (e.Data == null ? string.Empty : e.Data));

                        if (!p.HasExited)
                        {
                            p.Close();
                            p.Dispose();
                        }
                    }
                };

                p.Exited += (s, e) =>
                {
                    try
                    {
                        LogWriter.Write("作业分割进程退出:" + p.ExitCode);
                    }
                    catch (Exception ex)
                    {
                        LogWriter.Write("作业分割进程退出处理失败:" + ex.Message);
                    }
                };

                p.Start();
                p.BeginOutputReadLine();
                p.BeginErrorReadLine();
                //p.WaitForExit();
            }
            catch (Exception ex)
            {
                LogWriter.Write(string.Format("CallJobSplitMethod异常:{0},{1}", ex.Message, ex.StackTrace));
            }
        }

        /// <summary>
        /// 为当前作业创建作业空间
        /// </summary>
        public void CreateJobSpace()
        {
            // 清除目录下的内容
            try
            {
                Directory.Delete(CurrentJobSpacePath, true);
            }
            catch (DirectoryNotFoundException ex)
            {
            }

            // 当前作业文件夹
            if (!Directory.Exists(CurrentJobSpacePath))
            {
                Directory.CreateDirectory(CurrentJobSpacePath);
            }

            // 将程序包解压到当前作业的目录
            var pkgPath = Path.Combine(JobRootPath, Name + ".zip");
            if (!Directory.Exists(pkgPath))
            {
                ZipFile.ExtractToDirectory(pkgPath, CurrentJobSpacePath);
            }

            // 保存当前作业配置
            WriteJobSpaceConfig();
        }

        /// <summary>
        /// 生成当前作业的任务，由具体的作业调用此方法
        /// </summary>
        public void GenerateTasks()
        {
            if (!Directory.Exists(CurrentJobSpacePath))
            {
                Console.Write("GenerateTasks:Error:作业空间目录不存在");
                return;
            }

            // 保存任务创建状态文件
            string taskCreateStatusFilePath = Path.Combine(CurrentJobSpacePath, "taskcreate.status");
            File.WriteAllText(taskCreateStatusFilePath, "1");

            try
            {
                var tasks = Split();

                // 保存任务文件
                foreach (var task in tasks)
                {
                    task.Job = new JobWrapper(this);
                    task.WriteConfig();
                    task.WriteRequirement();
                }

                Console.WriteLine("GenerateTasks:OK");
            }
            catch (Exception ex)
            {
                Console.WriteLine("GenerateTasks:Error:" + ex.Message);
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
        /// 阻塞检查作业分割状态
        /// </summary>
        /// <returns></returns>
        private bool BlockCheckJobSplitStatus()
        {
            bool isJobSplitOK = true;
            int errorCode;
            string errorMessage;
            while (!CheckJobSplitStatus(out errorCode, out errorMessage))
            {
                LogWriter.Write(errorMessage);

                if (errorCode == 5)
                {
                    isJobSplitOK = false;
                    break;
                }

                Thread.Sleep(3000);
            }

            return isJobSplitOK;
        }

        /// <summary>
        /// 检查作业分割状态
        /// </summary>
        /// <param name="errorCode">0任务已创建完毕，1作业目录未创建，2任务创建还未开始，3不能读取任务创建状态文件，4任务正在创建，5任务处理出错</param>
        /// <param name="errorMessage"></param> 
        /// <returns></returns>
        private bool CheckJobSplitStatus(out int errorCode, out string errorMessage)
        {
            errorCode = -1;
            errorMessage = string.Empty;

            if (!Directory.Exists(CurrentJobSpacePath))
            {
                errorMessage = "作业创建等待中...";
                errorCode = 1;
                return false;
            }

            // 读取任务创建状态
            string physicalPath = Path.Combine(CurrentJobSpacePath, "taskcreate.status");
            if (!File.Exists(physicalPath))
            {
                errorMessage = "任务创建等待中...";
                errorCode = 2;
                return false;
            }

            var taskCreateStatus = "";
            try
            {
                taskCreateStatus = File.ReadAllText(physicalPath);
            }
            catch (Exception ex)
            {
                errorMessage = "读取任务创建状态异常：" + ex.Message;
                errorCode = 3;
                return false;
            }

            // 正在写入
            if (taskCreateStatus == "1")
            {
                errorMessage = "任务创建中...";
                errorCode = 4;
                return false;
            }

            // 任务处理出错
            if (taskCreateStatus.StartsWith("-1"))
            {
                errorMessage = "创建任务出错：" + taskCreateStatus;
                errorCode = 5;
                return false;
            }

            errorMessage = "任务创建完毕。";
            errorCode = 0;

            return true;
        }
        #endregion

        #region 运行任务
        /// <summary>
        /// 运行任务
        /// </summary>
        /// <param name="task"></param>
        public void RunTask(JobTask task)
        {
            PullTaskRequirement(task);

            // 开启处理任务
            StartRunTask(task);

            // 调用任务执行方法
            CallTaskExecuteMethod(task);

            // 阻塞并检查任务执行状态
            bool isExecuteOK = BlockCheckTaskExecuteStatus(task);

            if (isExecuteOK)
            {
                task.UpdateTaskStatus(EnumTaskStatus.Completed);
                LogWriter.Write(string.Format("任务执行成功:{0},{1},{2}", Name, Id, task.Id));
            }
            else
            {
                task.UpdateTaskStatus(EnumTaskStatus.Failed);
                LogWriter.Write(string.Format("任务执行失败:{0},{1},{2}", Name, Id, task.Id));
            }
        }

        /// <summary>
        /// 拉取任务需求
        /// </summary>
        /// <param name="task"></param>
        private void PullTaskRequirement(JobTask task)
        {
            // task目录
            var taskPath = Path.Combine(task.Job.CurrentJobSpacePath, "tasks", task.Id.ToString());
            if (!Directory.Exists(taskPath))
            {
                Directory.CreateDirectory(taskPath);
            }

            // task配置
            var taskJsonPath = Path.Combine(taskPath, "task.json");
            if (!File.Exists(taskJsonPath))
            {
                task.WriteConfig();
            }

            // 需求文件
            var taskRequirementPath = Path.Combine(taskPath, "requirement.txt");
            if (!File.Exists(taskRequirementPath))
            {
                Cluster.CurrentMember.Download(Cluster.Manager, "download/task/requirement",
                    string.Format("Jobs/{0}/{1}/tasks/{2}/requirement.txt", task.Job.Name, task.Job.Id, task.Id));
            }
        }

        /// <summary>
        /// 开始处理任务
        /// </summary>
        /// <param name="task"></param>
        private void StartRunTask(JobTask task)
        {
            task.UpdateTaskStatus(EnumTaskStatus.Executing);
            UpdateJobStatus(EnumJobRecordStatus.TaskExecuting);
        }

        /// <summary>
        /// 调用任务执行方法
        /// </summary>
        public void CallTaskExecuteMethod(JobTask task)
        {
            try
            {
                Process p = new Process();

                // 当前作业文件夹
                var currentJobStartPath = Path.Combine(CurrentJobSpacePath, FileName);
                var currentTaskPath = Path.Combine(CurrentJobSpacePath, "tasks", task.Id.ToString());

                string m_cmdLine = string.Format(" -p -t {0}", task.Id);
                p.StartInfo.WorkingDirectory = CurrentJobSpacePath;
                p.StartInfo.FileName = currentJobStartPath;
                p.StartInfo.Arguments = currentJobStartPath + m_cmdLine;
                p.StartInfo.UseShellExecute = false;
                p.StartInfo.RedirectStandardInput = true;
                p.StartInfo.RedirectStandardOutput = true;
                p.StartInfo.RedirectStandardError = true;
                p.StartInfo.CreateNoWindow = true;
                p.EnableRaisingEvents = true;

                p.OutputDataReceived += (s, e) =>
                {
                    if (e.Data != null && e.Data.StartsWith("PerformTask"))
                    {
                        if (e.Data.StartsWith("PerformTask:OK"))
                        {
                            string physicalPath = Path.Combine(currentTaskPath, "taskexecute.status");
                            File.WriteAllText(physicalPath, "0");
                        }

                        if (e.Data.StartsWith("PerformTask:Error"))
                        {
                            string physicalPath = Path.Combine(currentTaskPath, "taskexecute.status");
                            File.WriteAllText(physicalPath, "-1:" + e.Data);
                        }

                        if (!p.HasExited)
                        {
                            p.Close();
                            p.Dispose();
                        }
                    }
                };

                p.ErrorDataReceived += (s, e) =>
                {
                    if (e.Data != null)
                    {
                        string physicalPath = Path.Combine(currentTaskPath, "taskexecute.status");
                        File.WriteAllText(physicalPath, "-1:" + e.Data);

                        if (!p.HasExited)
                        {
                            p.Close();
                            p.Dispose();
                        }
                    }
                };

                p.Exited += (s, e) =>
                {
                    try
                    {
                        LogWriter.Write("任务执行进程退出:" + p.ExitCode);
                    }
                    catch (Exception ex)
                    {
                        LogWriter.Write("任务执行进程退出处理失败:" + ex.Message);
                    }
                };

                p.Start();
                p.BeginOutputReadLine();
                p.BeginErrorReadLine();
                //p.WaitForExit();
            }
            catch (Exception ex)
            {
                LogWriter.Write(string.Format("CallTaskExecuteMethod异常:{0},{1}", ex.Message, ex.StackTrace));
            }
        }

        /// <summary>
        /// 执行任务，由具体的作业调用此方法
        /// </summary>
        /// <param name="task"></param>
        public void PerformTask(JobTask task)
        {
            // 保存任务创建状态文件
            string taskCreateStatusFilePath = Path.Combine(CurrentJobSpacePath, "tasks", task.Id.ToString(), "taskexecute.status");
            File.WriteAllText(taskCreateStatusFilePath, "1");

            try
            {
                var result = ExecuteTask(task);
                task.Result = result;
                task.WriteResult();

                Console.WriteLine("PerformTask:OK");
            }
            catch (System.Exception ex)
            {
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
        /// 阻塞检查任务执行状态
        /// </summary>
        /// <param name="task"></param>
        /// <returns></returns>
        private bool BlockCheckTaskExecuteStatus(JobTask task)
        {
            bool isOK = true;

            int errorCode;
            string errorMessage;
            while (!CheckTaskExecuteStatus(task, out errorCode, out errorMessage))
            {
                LogWriter.Write(errorMessage);

                if (errorCode == 4)
                {
                    isOK = false;
                    break;
                }

                Thread.Sleep(3000);
            }

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
            errorCode = -1;
            errorMessage = string.Empty;

            // 读取任务创建状态
            string physicalPath = Path.Combine(CurrentJobSpacePath, "tasks", task.Id.ToString(), "taskexecute.status");
            if (!File.Exists(physicalPath))
            {
                errorMessage = "任务执行等待中...";
                errorCode = 1;
                return false;
            }

            var taskExecuteStatus = "";
            try
            {
                taskExecuteStatus = File.ReadAllText(physicalPath);
            }
            catch (Exception ex)
            {
                errorMessage = "读取任务执行状态异常：" + ex.Message;
                errorCode = 2;
                return false;
            }

            // 正在写入
            if (taskExecuteStatus == "1")
            {
                errorMessage = "任务执行中...";
                errorCode = 3;
                return false;
            }

            // 任务处理出错
            if (taskExecuteStatus.StartsWith("-1"))
            {
                errorMessage = "任务执行出错：" + taskExecuteStatus;
                errorCode = 4;
                return false;
            }

            errorMessage = "任务执行完毕。";
            errorCode = 0;

            return true;
        }
        #endregion

        #region 任务结果处理
        /// <summary>
        /// 同步任务结果
        /// </summary>
        public void SyncTaskResult()
        {
            var taskPlan = TaskPlan;

            if (taskPlan != null && taskPlan.Count > 0)
            {
                var executeCompleteTasks = taskPlan.SelectMany(d => d.Value.Where(t => t.Status == EnumTaskStatus.Completed));
                if (executeCompleteTasks.Any())
                {
                    foreach (var task in executeCompleteTasks)
                    {
                        try
                        {
                            PullTaskResult(task);
                            task.UpdateTaskStatus(EnumTaskStatus.Synced);
                        }
                        catch (System.Exception ex)
                        {
                            task.UpdateTaskStatus(EnumTaskStatus.SyncFailed);
                            LogWriter.Write(string.Format("同步任务结果异常:{0}", ex.Message));
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 拉取任务结果
        /// </summary>
        /// <param name="task"></param>
        private void PullTaskResult(JobTask task)
        {
            // 任务所属成员
            var memberId = TaskPlan.Where(d => d.Value.Where(t => t.Id == task.Id).Any()).Select(d => d.Key).FirstOrDefault();
            var member = Cluster.Members.Where(d => d.Id == memberId).FirstOrDefault();
            if (member == null)
            {
                throw new MemberNotFoundException(string.Format("成员[{0}]不存在，无法拉取任务结果。", memberId));
            }

            // 下载任务结果
            Cluster.CurrentMember.Download(member, "download/task/result",
                string.Format("Jobs/{0}/{1}/tasks/{2}/result.txt", task.Job.Name, task.Job.Id, task.Id));
        }

        /// <summary>
        /// 检查任务运行状态
        /// </summary>
        public void CheckTaskRunStatus()
        {
            if (TaskPlan == null)
            {
                LogWriter.Write(string.Format("任务计划为空:{0},{1}", Name, Id));
                return;
            }
            var taskCount = TaskPlan.SelectMany(d => d.Value).Count();
            var executeCompleteTaskCount = TaskPlan.SelectMany(d => d.Value.Where(t => t.Status == EnumTaskStatus.Completed)).Count();
            var SyncCompleteTaskCount = TaskPlan.SelectMany(d => d.Value.Where(t => t.Status == EnumTaskStatus.Synced)).Count();

            if (taskCount == SyncCompleteTaskCount)
            {
                UpdateJobStatus(EnumJobRecordStatus.TaskSynced);
            }
            else if (taskCount == executeCompleteTaskCount)
            {
                UpdateJobStatus(EnumJobRecordStatus.TaskCompleted);
            }
        }

        /// <summary>
        /// 合并任务处理结果
        /// </summary>
        public void MergeTaskResult()
        {
            // 保存任务合并状态文件
            string taskCreateStatusFilePath = Path.Combine(CurrentJobSpacePath, "taskmerge.status");
            File.WriteAllText(taskCreateStatusFilePath, "1");

            CallTaskCollectMethod();

            // 阻塞并检查任务结果合并进度
            bool isMergeOK = BlockCheckTaskResultMergeStatus();

            if (isMergeOK)
            {
                UpdateJobStatus(EnumJobRecordStatus.TaskMerged);
                LogWriter.Write(string.Format("任务合并成功:{0},{1}", Name, Id));
            }
            else
            {
                UpdateJobStatus(EnumJobRecordStatus.TaskMergeFailed);
                LogWriter.Write(string.Format("任务合并失败:{0},{1}", Name, Id));
            }
        }

        /// <summary>
        /// 合并所有任务结果
        /// </summary>
        public void CollectTaskResults()
        {
            try
            {
                var tasks = TaskPlan.SelectMany(d => d.Value);
                foreach (var task in tasks)
                {
                    task.LoadResult();
                }
                LogWriter.Write("已经准备好要合并的任务");

                string result = Collect(tasks);
                LogWriter.Write("已经汇集完毕任务结果");

                string jobResultPath = Path.Combine(CurrentJobSpacePath, "result.txt");
                File.WriteAllText(jobResultPath, result);
                LogWriter.Write("已经汇集完毕任务结果");

                Console.WriteLine("CollectTaskResults:OK");
            }
            catch (Exception ex)
            {
                Console.WriteLine("CollectTaskResults:Error:" + ex.Message + ex.StackTrace);
            }
        }

        /// <summary>
        /// 阻塞检查任务结果合并状态
        /// </summary>
        /// <returns></returns>
        private bool BlockCheckTaskResultMergeStatus()
        {
            bool taskOK = true;
            int unMergeReason;
            string unMergeReasonDesc;
            while (!CheckTaskMergeResultStatus(out unMergeReason, out unMergeReasonDesc))
            {
                LogWriter.Write(unMergeReasonDesc);

                if (unMergeReason == 4)
                {
                    taskOK = false;
                    break;
                }

                Thread.Sleep(3000);
            }

            return taskOK;
        }

        /// <summary>
        /// 检查任务结果合并状态
        /// </summary>
        /// <param name="errorCode">1尚未创建合并状态，2不能读取任务合并状态，3任务合并中，4任务合并出错</param>
        /// <returns></returns>
        private bool CheckTaskMergeResultStatus(out int errorCode, out string errorMessage)
        {
            errorCode = -1;
            errorMessage = string.Empty;

            // 读取任务合并状态
            string physicalPath = Path.Combine(CurrentJobSpacePath, "taskmerge.status");
            if (!File.Exists(physicalPath))
            {
                errorMessage = "任务合并等待中...";
                errorCode = 1;
                return false;
            }

            var taskMergeStatus = "";
            try
            {
                taskMergeStatus = File.ReadAllText(physicalPath);
            }
            catch (Exception ex)
            {
                errorMessage = "读取任务创建状态异常：" + ex.Message;
                errorCode = 2;
                return false;
            }

            // 正在合并
            if (taskMergeStatus == "1")
            {
                errorMessage = "任务合并中...";
                errorCode = 3;
                return false;
            }

            // 任务处理出错
            if (taskMergeStatus.StartsWith("-1"))
            {
                errorMessage = "创建合并出错：" + taskMergeStatus;
                errorCode = 4;
                return false;
            }

            errorMessage = "任务合并完毕。";
            errorCode = 0;

            return true;
        }

        /// <summary>
        /// 调用任务合并方法，由Manager调用此方法
        /// </summary>
        public void CallTaskCollectMethod()
        {
            try
            {
                Process p = new Process();

                // 当前作业文件夹
                var currentJobStartPath = Path.Combine(CurrentJobSpacePath, FileName);

                string m_cmdLine = string.Format(" -m");
                p.StartInfo.WorkingDirectory = CurrentJobSpacePath;
                p.StartInfo.FileName = currentJobStartPath;
                p.StartInfo.Arguments = currentJobStartPath + m_cmdLine;
                p.StartInfo.UseShellExecute = false;
                p.StartInfo.RedirectStandardInput = true;
                p.StartInfo.RedirectStandardOutput = true;
                p.StartInfo.RedirectStandardError = true;
                p.StartInfo.CreateNoWindow = true;
                p.EnableRaisingEvents = true;

                p.OutputDataReceived += (s, e) =>
                {
                    if (e.Data != null)
                    {
                        if (e.Data.StartsWith("CollectTaskResults"))
                        {
                            if (e.Data.StartsWith("CollectTaskResults:OK"))
                            {
                                string physicalPath = Path.Combine(CurrentJobSpacePath, "taskmerge.status");
                                File.WriteAllText(physicalPath, "0");
                            }

                            if (e.Data.StartsWith("CollectTaskResults:Error"))
                            {
                                string physicalPath = Path.Combine(CurrentJobSpacePath, "taskmerge.status");
                                File.WriteAllText(physicalPath, "-1:" + e.Data);
                            }

                            if (!p.HasExited)
                            {
                                p.Close();
                                p.Dispose();
                            }
                        }
                        else
                        {
                            LogWriter.Write(e.Data);
                        }
                    }
                };

                p.ErrorDataReceived += (s, e) =>
                {
                    if (e.Data != null)
                    {
                        string physicalPath = Path.Combine(CurrentJobSpacePath, "taskmerge.status");
                        File.WriteAllText(physicalPath, "-1:" + e.Data);

                        if (!p.HasExited)
                        {
                            p.Close();
                            p.Dispose();
                        }
                    }
                };

                p.Exited += (s, e) =>
                {
                    try
                    {
                        LogWriter.Write("任务执行进程退出:" + p.ExitCode);
                    }
                    catch (Exception ex)
                    {
                        LogWriter.Write("任务执行进程退出处理失败:" + ex.Message);
                    }
                };

                p.Start();
                p.BeginOutputReadLine();
                p.BeginErrorReadLine();
                //p.WaitForExit();
            }
            catch (Exception ex)
            {
                LogWriter.Write(string.Format("CallTaskCollectMethod异常:{0},{1}", ex.Message, ex.StackTrace));
            }
        }

        /// <summary>
        /// 汇集各个任务的处理结果
        /// 如果结果数据量很大，建议将结果保存到某个地方，这里只返回结果的索引
        /// </summary>
        /// <returns>作业处理结果</returns>
        public abstract string Collect(IEnumerable<JobTask> tasks);
        #endregion

        /// <summary>
        /// 更新作业状态
        /// </summary>
        /// <param name="status"></param>
        private void UpdateJobStatus(EnumJobRecordStatus status)
        {
            Status = status;

            // 可能同时更新作业记录配置，所以这里用CAS
            KVPair jobRecordKV;
            do
            {
                var jobRecordKey = string.Format("Swift/{0}/Jobs/{1}/Records/{2}", Cluster.Name, Name, Id);
                jobRecordKV = ConsulKV.Get(jobRecordKey);
                var jobRecord = JsonConvert.DeserializeObject<JobWrapper>(Encoding.UTF8.GetString(jobRecordKV.Value));
                jobRecordKV.Value = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(this));
            } while (!ConsulKV.CAS(jobRecordKV));
        }

        /// <summary>
        /// 加载作业的任务
        /// </summary>
        public List<JobTask> LoadTasksFromFile()
        {
            var taskList = new List<JobTask>();

            var taskPath = Path.Combine(CurrentJobSpacePath, "tasks");

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
                    var taskJsonPath = Path.Combine(taskDir, "task.json");
                    var taskJson = File.ReadAllText(taskJsonPath);
                    var task = JsonConvert.DeserializeObject<JobTask>(taskJson);
                    taskList.Add(task);
                }
            }

            return taskList;
        }

        /// <summary>
        /// 读取为字节数组
        /// </summary>
        /// <returns></returns>
        public byte[] ReadFile()
        {
            if (FileBytes == null)
            {
                string physicalPath = Path.Combine(JobRootPath, Name + ".zip");
                FileBytes = ReadFile(physicalPath);
            }

            return FileBytes;
        }

        /// <summary>
        /// 读取文件到字节数组
        /// </summary>
        /// <param name="fileUrl"></param>
        /// <returns></returns>
        private byte[] ReadFile(string fileUrl)
        {
            FileStream fs = new FileStream(fileUrl, FileMode.Open, FileAccess.Read);
            try
            {
                byte[] buffur = new byte[fs.Length];
                fs.Read(buffur, 0, (int)fs.Length);

                return buffur;
            }
            catch (Exception ex)
            {
                Console.WriteLine(string.Format("{0} 读取作业文件异常:{1}", DateTime.Now.ToString(), ex.Message));
                return null;
            }
            finally
            {
                if (fs != null)
                {
                    fs.Close();
                }
            }
        }

        /// <summary>
        /// 写作业空间配置
        /// </summary>
        private void WriteJobSpaceConfig()
        {
            // 保存当前作业配置
            var jobConfigJson = JsonConvert.SerializeObject(this);
            string currentJobConfigFilePath = Path.Combine(CurrentJobSpacePath, "job.json");
            File.WriteAllText(currentJobConfigFilePath, jobConfigJson);
        }
    }
}
