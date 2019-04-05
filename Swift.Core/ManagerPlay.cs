using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Swift.Core.Log;

namespace Swift.Core
{
    /// <summary>
    /// 管理员剧本
    /// </summary>
    public class ManagerPlay
    {
        private Member _member;
        private Cluster _cluster;
        private readonly int _executeAmountLimit;
        private Thread _jobProcessThread;
        private CancellationTokenSource _jobProcessThreadCts;
        private Thread _cleanExitedSystemTaskThread;
        private CancellationTokenSource _cleanExitedSystemTaskThreadCts;
        private Thread _cleanOutOfControlChildProcessThread;
        private CancellationTokenSource _cleanOutOfControlChildProcessThreadCts;
        private readonly ConcurrentDictionary<string, Task> _activedJobs = new ConcurrentDictionary<string, Task>();

        /// <summary>
        /// 构造函数
        /// </summary>
        public ManagerPlay(Member member)
        {
            _member = member;
            _cluster = _member.Cluster;
            _executeAmountLimit = Environment.ProcessorCount;
        }

        /// <summary>
        /// 开始工作
        /// </summary>
        public void Start()
        {
            LogWriter.Write("manager play starting ...");

            // 启动的时候先清理一次，然后循环执行
            CleanOutOfControlChildProcess();

            _cluster.MonitorMembersHealth();
            _cluster.MonitorJobConfigsFromDisk();
            _cluster.MonitorJobConfigsFromConfigCenter();
            _cluster.MonitorJobCreate();

            _jobProcessThreadCts = new CancellationTokenSource();
            StartProcessJobs(_jobProcessThreadCts.Token);

            _cleanExitedSystemTaskThreadCts = new CancellationTokenSource();
            StartCleanExitedSystemTasks(_cleanExitedSystemTaskThreadCts.Token);

            _cleanOutOfControlChildProcessThreadCts = new CancellationTokenSource();
            StartCleanOutOfControlChildProcess(_cleanOutOfControlChildProcessThreadCts.Token);

            LogWriter.Write("manager play started");
        }

        /// <summary>
        /// 停止工作
        /// </summary>
        public void Stop()
        {
            LogWriter.Write("manager play stopping ...");

            _cluster.StopMonitorJobCreate();
            _cluster.StopMonitorJobConfigsFromDisk();
            _cluster.StopMonitorJobConfigsFromConfigCenter();
            _cluster.StopMonitorMemberHealth();

            _jobProcessThreadCts.Cancel();
            if (_jobProcessThread != null)
            {
                try
                {
                    _jobProcessThread.Join();
                }
                catch (Exception ex)
                {
                    LogWriter.Write("wait for job process thread exit go exception", ex, LogLevel.Warn);
                }
            }
            LogWriter.Write("clean job process thread has exited");

            _cleanExitedSystemTaskThreadCts.Cancel();
            if (_cleanExitedSystemTaskThread != null)
            {
                try
                {
                    _cleanExitedSystemTaskThread.Join();
                }
                catch (Exception ex)
                {
                    LogWriter.Write("wait for clean system task thread exit go exception", ex, LogLevel.Warn);
                }
            }
            LogWriter.Write("clean system task thread has exited");

            _cleanOutOfControlChildProcessThreadCts.Cancel();
            if (_cleanOutOfControlChildProcessThread != null)
            {
                try
                {
                    _cleanOutOfControlChildProcessThread.Join();
                }
                catch (Exception ex)
                {
                    LogWriter.Write("wait for clean out of control process thread exit go exception", ex, LogLevel.Warn);
                }
            }
            LogWriter.Write("clean out of control process thread has exited");

            // 经过上边的处理，应该不会有未处理的进程了，
            // 除非Stop的过程中程序退出了，但是剧本上线的时候还会继续检查不受控制的进程
            // KillChildProcess();

            LogWriter.Write("manager play stopped");
        }

        /// <summary>
        /// 强制杀死所有子进程
        /// </summary>
        private void KillChildProcess()
        {
            if (_activedJobs.Count > 0)
            {
                foreach (var jobId in _activedJobs.Keys)
                {
                    var job = (JobBase)_activedJobs[jobId].AsyncState;

                    if (job.HasRelatedProcess)
                    {
                        LogWriter.Write("kill job child process because stop");
                        job.KillRelatedProcess();
                    }

                    // 如果有正在进行的作业，强制回滚到初始状态
                    // 回滚到其它状态涉及到状态同步和数据同步，似乎都比较麻烦
                    if (job.Status != EnumJobRecordStatus.Pending)
                    {
                        job.TaskPlan = null;
                        job.UpdateJobStatus(EnumJobRecordStatus.Pending);
                    }
                }
            }
        }

        /// <summary>
        /// 启动清理脱离控制的子进程
        /// </summary>
        private void StartCleanOutOfControlChildProcess(CancellationToken cancellationToken)
        {
            _cleanOutOfControlChildProcessThread = new Thread(() =>
            {
                while (true)
                {
                    Thread.Sleep(6000);

                    try
                    {
                        // 这里没有传递cancellationToken，清理工作，不希望取消，尽量都执行
                        CleanOutOfControlChildProcess();
                    }
                    catch (Exception ex)
                    {
                        LogWriter.Write("清理脱离控制的子进程异常", ex);
                    }

                    // 放在清理后边执行就是为了尽可能的多清理一次
                    if (cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }
                }
            })
            {
                Name = "CleanOutOfControlChildProcessThread",
                IsBackground = true
            };

            _cleanOutOfControlChildProcessThread.Start();
        }

        /// <summary>
        /// 清理脱离控制的子进程
        /// 进程的关闭会导致系统任务的退出，所以这里不用关心系统任务的清理
        /// </summary>
        private void CleanOutOfControlChildProcess()
        {
            LogWriter.Write("开始清理脱离控制的子进程...", LogLevel.Trace);

            var processList = SwiftProcess.GetAllLocalProcess();

            // 处理作业分割进程
            var jobSplitProcess = processList.Where(d => d.GetValue(0).ToString() == "SplitJob");
            if (jobSplitProcess.Any())
            {
                CleanOutOfControlJobSplitProcess(jobSplitProcess);
            }

            // 处理任务合并进程
            var collectTaskResultProcess = processList.Where(d => d.GetValue(0).ToString() == "CollectTaskResult");
            if (collectTaskResultProcess.Any())
            {
                CleanOutOfControlCollectTaskResultProcess(collectTaskResultProcess);
            }

            // 获取正在运行的任务进程
            var taskExecuteProcess = processList.Where(d => d.GetValue(0).ToString() == "ExecuteTask");
            if (taskExecuteProcess.Any())
            {
                CleanOutOfControlTaskExecuteProcess(taskExecuteProcess);
            }

            LogWriter.Write("完成清理脱离控制的子进程");
        }

        /// <summary>
        /// 清理脱离控制的执行任务进程
        /// </summary>
        /// <param name="taskExecuteProcess">Task execute process.</param>
        private void CleanOutOfControlTaskExecuteProcess(IEnumerable<string[]> taskExecuteProcess)
        {
            LogWriter.Write("Manager不应该执行任务进程，他们都应该被Kill");

            foreach (var processInfo in taskExecuteProcess)
            {
                var processId = int.Parse(processInfo[1]);
                var jobName = processInfo[2];
                var jobId = processInfo[3];
                var taskId = int.Parse(processInfo[4]);

                LogWriter.Write(string.Format("正在处理：{0},{1},{2}", jobName, jobId, taskId));
                SwiftProcess.KillAbandonedTaskProcess(processId, jobName, jobId, taskId);
            }
        }

        /// <summary>
        /// 清理脱离控制的任务结果合并进程
        /// </summary>
        /// <param name="collectTaskResultProcess">Collect task result process.</param>
        private void CleanOutOfControlCollectTaskResultProcess(IEnumerable<string[]> collectTaskResultProcess, CancellationToken cancellationToken = default(CancellationToken))
        {
            foreach (var processInfo in collectTaskResultProcess)
            {
                var processId = int.Parse(processInfo[1]);
                var jobName = processInfo[2];
                var jobId = processInfo[3];

                LogWriter.Write(string.Format("正在处理：{0},{1}", jobName, jobId));

                var jobRecord = _cluster.ConfigCenter.GetJobRecord(jobName, jobId, _cluster, cancellationToken);

                // 作业不存在了，看看作业任务合并进程还在不在
                if (jobRecord == null)
                {
                    LogWriter.Write("作业记录不存在了，尝试关闭废弃的任务合并进程");
                    SwiftProcess.KillAbandonedCollectTaskResultProcess(processId, jobName, jobId);
                    continue;
                }
                LogWriter.Write(string.Format("作业记录存在"));

                // 任务非TaskMerging状态，看看进程在不在
                if (jobRecord.Status != EnumJobRecordStatus.TaskMerging)
                {
                    LogWriter.Write("任务非TaskMerging状态，尝试关闭废弃的任务合并进程");
                    SwiftProcess.KillAbandonedCollectTaskResultProcess(processId, jobName, jobId);
                    continue;
                }
                LogWriter.Write(string.Format("作业在TaskMerging状态，将继续运行"));
            }
        }

        /// <summary>
        /// 清理脱离控制的作业分割进程
        /// </summary>
        /// <param name="jobSplitProcess">Job split process.</param>
        private void CleanOutOfControlJobSplitProcess(IEnumerable<string[]> jobSplitProcess, CancellationToken cancellationToken = default(CancellationToken))
        {
            foreach (var processInfo in jobSplitProcess)
            {
                var processId = int.Parse(processInfo[1]);
                var jobName = processInfo[2];
                var jobId = processInfo[3];

                LogWriter.Write(string.Format("正在处理：{0},{1}", jobName, jobId));

                var jobRecord = _cluster.ConfigCenter.GetJobRecord(jobName, jobId, _cluster, cancellationToken);

                // 作业不存在了，看看作业分割进程还在不在
                if (jobRecord == null)
                {
                    LogWriter.Write("作业记录不存在了，尝试关闭废弃的作业分割进程");
                    SwiftProcess.KillAbandonedJobSplitProcess(processId, jobName, jobId);
                    continue;
                }
                LogWriter.Write(string.Format("作业记录存在"));

                // 任务非PlanMaking状态，看看进程在不在
                if (jobRecord.Status != EnumJobRecordStatus.PlanMaking)
                {
                    LogWriter.Write("任务非PlanMaking状态，尝试关闭废弃的作业分割进程");
                    SwiftProcess.KillAbandonedJobSplitProcess(processId, jobName, jobId);
                    continue;
                }
                LogWriter.Write(string.Format("作业在PlanMaking状态，将继续运行"));
            }
        }

        /// <summary>
        /// 开启处理作业
        /// </summary>
        private void StartProcessJobs(CancellationToken cancellationToken)
        {
            _jobProcessThread = new Thread(() =>
            {
                while (true)
                {
                    try
                    {
                        ProcessJobs(cancellationToken);
                    }
                    catch (System.OperationCanceledException ex)
                    {
                        LogWriter.Write("process jobs thread is canceled", ex, LogLevel.Info);
                        break;
                    }
                    catch (Exception ex)
                    {
                        LogWriter.Write("unknown exception occur when process jobs", ex);
                    }

                    if (cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }

                    Thread.Sleep(6000);
                }
            })
            {
                Name = "jobProcessThread",
                IsBackground = true
            };

            _jobProcessThread.Start();
        }

        /// <summary>
        /// 处理作业
        /// </summary>
        private void ProcessJobs(CancellationToken cancellationToken = default(CancellationToken))
        {
            var jobs = _cluster.GetLatestJobRecords(cancellationToken);
            if (jobs.Length <= 0)
            {
                LogWriter.Write("no job be happy", LogLevel.Debug);
                return;
            }

            var workers = _cluster.GetLatestWorkers(cancellationToken);
            if (workers == null || !workers.Any(d => d.Status == 1))
            {
                LogWriter.Write("no worker do nothing", LogLevel.Debug);
                return;
            }

            LogWriter.Write(string.Format("the number of jobs: {0}", jobs.Length), LogLevel.Debug);

            // 先处理取消中的作业
            ProcessCancelingJobs(jobs);

            // 优先处理运行中状态的任务，因为这些作业的进程可能已经不受控制了，需要尽快处理
            jobs = OrderJobsByStatus(jobs);

            var hasProcessAmount = 0;
            while (hasProcessAmount < jobs.Length)
            {
                for (int i = hasProcessAmount; i < jobs.Length; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (CheckExecutingAmountReachLimit())
                    {
                        LogWriter.Write("作业激活数已达上限。");
                        break;
                    }

                    var job = jobs[i];

                    if (_activedJobs.TryGetValue(job.BusinessId, out Task jobTask))
                    {
                        hasProcessAmount++;
                        LogWriter.Write("作业循环检测到作业还在处理：" + job.BusinessId, Log.LogLevel.Trace);
                        continue;
                    }

                    _activedJobs.TryAdd(job.BusinessId, Task.Factory.StartNew(j => ProcessJob((JobBase)j, cancellationToken), job, cancellationToken));
                    LogWriter.Write("作业循环已将作业激活：" + job.BusinessId, Log.LogLevel.Info);

                    hasProcessAmount++;
                }

                if (hasProcessAmount < jobs.Length)
                {
                    Thread.Sleep(3000);
                }
            }

            LogWriter.Write("作业循环已遍历当前所有作业。");
        }

        /// <summary>
        /// 处理取消中的作业
        /// </summary>
        /// <param name="jobs">Jobs.</param>
        private void ProcessCancelingJobs(JobBase[] jobs)
        {
            var cancelingJobs = jobs.Where(d => d.Status == EnumJobRecordStatus.Canceling).ToArray();
            if (cancelingJobs.Length > 0)
            {
                LogWriter.Write(string.Format("found canceling jobs: {0}", cancelingJobs.Length));
                Array.ForEach(cancelingJobs, ProcessCancelingJob);
            }
        }

        /// <summary>
        /// 处理取消中的单个作业
        /// </summary>
        private void ProcessCancelingJob(JobBase job)
        {
            // 移除正在运行的系统任务，及杀掉相关子进程
            if (_activedJobs.TryRemove(job.BusinessId, out Task sysTask))
            {
                var runningJob = (JobBase)sysTask.AsyncState;
                if (runningJob.HasRelatedProcess)
                {
                    LogWriter.Write("prepare kill job process because job is canceling");
                    runningJob.KillRelatedProcess();
                }
            }
        }

        /// <summary>
        /// 根据状态排序作业
        /// </summary>
        /// <returns>The jobs by status.</returns>
        /// <param name="jobs">Jobs.</param>
        private static JobBase[] OrderJobsByStatus(JobBase[] jobs)
        {
            jobs = jobs.OrderBy(d => d.Status, Comparer<EnumJobRecordStatus>.Create((x, y) =>
            {
                if (x == EnumJobRecordStatus.TaskExecuting || x == EnumJobRecordStatus.TaskMerging)
                {
                    return -1;
                }

                if (x == y)
                {
                    return 0;
                }

                return 1;
            })).ToArray();
            return jobs;
        }

        /// <summary>
        /// 处理单个作业
        /// </summary>
        /// <param name="job">Job.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private void ProcessJob(JobBase job, CancellationToken cancellationToken = default(CancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            // 明确为计划制定失败、任务合并失败、任务执行失败的不应该重试，应该先找找原因。

            // 处理任务计划中下线的工人
            if (job.Status == EnumJobRecordStatus.PlanMaked
                || job.Status == EnumJobRecordStatus.TaskExecuting
                || job.Status == EnumJobRecordStatus.Canceling
                || job.Status == EnumJobRecordStatus.TaskCompleted)
            {
                job.ReplaceTaskPlanOfflineWorker(cancellationToken);
            }

            // 待处理的作业肯定需要处理
            if (job.Status == EnumJobRecordStatus.Pending)
            {
                job.CreateProductionPlan(cancellationToken);
            }

            // 正在制定计划的作业，可能Swift停止运行导致的
            if (job.Status == EnumJobRecordStatus.PlanMaking)
            {
                LogWriter.Write("作业为计划制定中状态，将检查对应的进程：" + job.BusinessId, Log.LogLevel.Info);

                if (CheckPlanMakingJob(job, cancellationToken))
                {
                    LogWriter.Write("开始监控运行作业计划制定");
                    job.MointorRunJobSplit(cancellationToken);
                }
            }

            // 计划制定完毕、任务正在执行或都处理完成的作业：同步任务结果
            if (job.Status == EnumJobRecordStatus.PlanMaked
            || job.Status == EnumJobRecordStatus.TaskExecuting
            || job.Status == EnumJobRecordStatus.TaskCompleted)
            {
                job.SyncTaskResult(cancellationToken);
            }

            // 任务正在执行或都处理完成的作业：更新作业状态为任务已全部完成或任务结果已全部同步
            if (job.Status == EnumJobRecordStatus.TaskExecuting
            || job.Status == EnumJobRecordStatus.TaskCompleted
            || job.Status == EnumJobRecordStatus.Canceling)
            {
                job.CheckTaskRunStatus(cancellationToken);
            }

            // 合并任务同步完成的作业：合并全部任务结果
            if (job.Status == EnumJobRecordStatus.TaskSynced)
            {
                job.MergeTaskResult(cancellationToken);
            }

            // 正在合并结果的作业，可能Swift停止运行导致的
            if (job.Status == EnumJobRecordStatus.TaskMerging)
            {
                LogWriter.Write("作业为任务结果合并中状态，将检查对应的进程：" + job.BusinessId, Log.LogLevel.Info);

                if (CheckTaskMergingJob(job, cancellationToken))
                {
                    LogWriter.Write("开始监控运行任务结果合并");
                    job.MointorRunCollectTaskResult(cancellationToken);
                }
            }
        }

        /// <summary>
        /// 检查状态为计划制定中的作业，如果需要继续运行则返回true，否则返回false
        /// </summary>
        /// <returns><c>true</c>, if plan making job was checked, <c>false</c> otherwise.</returns>
        /// <param name="job">Job.</param>
        private bool CheckPlanMakingJob(JobBase job, CancellationToken cancellationToken = default(CancellationToken))
        {
            int processId = job.GetProcessId("SplitJob");

            // 没有进程Id，则认为作业在启动进程前就挂掉了或者进程没有启动成功
            if (processId == -1)
            {
                LogWriter.Write("没有进程Id，启动作业分割进程前就挂掉了或者进程没有启动成功，将更新任务状态为初始");
                job.UpdateJobStatus(EnumJobRecordStatus.Pending, cancellationToken);
                return false;
            }

            // 获取进程
            SwiftProcess.TryGetById(processId, job, "SplitJob", out SwiftProcess process);

            // 如果状态文件标示已经执行完毕，则更新作业状态
            // 此时进程应该退出了，如果未退出可能是有未释放的资源，此时强行退出。
            var jobSplitStatus = job.GetJobSplitStatus();
            if (jobSplitStatus.ErrCode == 0)
            {
                LogWriter.Write("作业分割状态文件标示已经执行完毕，准备更新作业状态为计划制定完成");
                job.UpdateJobStatus(EnumJobRecordStatus.PlanMaked, cancellationToken);

                if (process != null)
                {
                    job.KillJobSplitProcess();
                }
                return false;
            }

            // 进程也没了，状态文件也不是完成，那还要分两种情况
            if (process == null)
            {
                if (jobSplitStatus.ErrCode == 2 || jobSplitStatus.ErrCode == 4)
                {
                    LogWriter.Write("找不到进程，状态为进行中或待执行，则回退到初始状态");
                    job.UpdateJobStatus(EnumJobRecordStatus.Pending, cancellationToken);
                }
                else
                {
                    LogWriter.Write("找不到进程，状态错误，则修改为失败状态，需要调查");
                    job.UpdateJobStatus(EnumJobRecordStatus.PlanFailed, cancellationToken);
                }

                return false;
            }

            // 如果根据进程Id找到进程，则启动监控运行作业
            return true;
        }

        /// <summary>
        /// 检查状态为任务合并中的作业，如果需要继续运行则返回true，否则返回false
        /// </summary>
        /// <returns><c>true</c>, if plan making job was checked, <c>false</c> otherwise.</returns>
        /// <param name="job">Job.</param>
        private bool CheckTaskMergingJob(JobBase job, CancellationToken cancellationToken = default(CancellationToken))
        {
            int processId = job.GetProcessId("CollectTaskResult");

            // 没有进程Id，则认为在启动进程前就挂掉了或者进程没有启动成功
            if (processId == -1)
            {
                LogWriter.Write("没有进程Id，启动任务合并进程前就挂掉了或者进程没有启动成功，将更新作业状态为TaskSynced");
                job.UpdateJobStatus(EnumJobRecordStatus.TaskSynced, cancellationToken);
                return false;
            }

            // 获取进程
            SwiftProcess.TryGetById(processId, job, "CollectTaskResult", out SwiftProcess process);

            // 如果状态文件标示已经执行完毕，则更新作业状态
            // 此时进程应该退出了，如果未退出可能是有未释放的资源，此时强行退出。
            var collectTaskResultStatus = job.GetCollectTaskResultStatus();
            if (collectTaskResultStatus.ErrCode == 0)
            {
                LogWriter.Write("任务合并状态文件标示已经执行完毕，准备更新作业状态为TaskMerged");
                job.UpdateJobStatus(EnumJobRecordStatus.TaskMerged, cancellationToken);

                if (process != null)
                {
                    job.KillCollectTaskResultProcess();
                }
                return false;
            }

            // 进程也没了，状态文件也不是完成，那还要分两种情况
            if (process == null)
            {
                if (collectTaskResultStatus.ErrCode == 2 || collectTaskResultStatus.ErrCode == 4)
                {
                    LogWriter.Write("找不到进程，状态为进行中或待执行，则回退到TaskSynced");
                    job.UpdateJobStatus(EnumJobRecordStatus.TaskSynced, cancellationToken);
                }
                else
                {
                    LogWriter.Write("找不到进程，状态为错误，则修改为TaskMergeFailed，需要调查");
                    job.UpdateJobStatus(EnumJobRecordStatus.TaskMergeFailed, cancellationToken);
                }

                return false;
            }

            // 如果根据进程Id找到进程，则启动监控运行作业
            return true;
        }

        /// <summary>
        /// 启动清理退出的系统任务
        /// </summary>
        private void StartCleanExitedSystemTasks(CancellationToken cancellationToken)
        {
            _cleanExitedSystemTaskThread = new Thread(() =>
            {
                while (true)
                {
                    Thread.Sleep(3000);

                    // 这里没有传递cancellationToken，清理工作，不希望取消，尽量都执行
                    CleanExitedSystemTask();

                    // 放在清理后边执行就是为了尽可能的多清理一次
                    if (cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }
                }
            })
            {
                Name = "CleanExitedSystemTaskThread",
                IsBackground = true,
            };

            _cleanExitedSystemTaskThread.Start();
        }

        /// <summary>
        /// 清理退出的系统任务
        /// </summary>
        private void CleanExitedSystemTask()
        {
            List<string> removeList = new List<string>();

            foreach (var jobId in _activedJobs.Keys)
            {
                var sysTask = _activedJobs[jobId];
                if (sysTask.Status == TaskStatus.RanToCompletion)
                {
                    removeList.Add(jobId);
                    LogWriter.Write("准备从激活池中移除处理完毕的作业：" + jobId);
                }

                if (sysTask.Status == TaskStatus.Canceled
                || sysTask.Status == TaskStatus.Faulted)
                {
                    removeList.Add(jobId);
                    LogWriter.Write("准备从激活池中移除取消或出现异常的作业：" + jobId);
                }

                //if (sysTask.Status == TaskStatus.RanToCompletion
                //    || sysTask.Status == TaskStatus.Canceled
                //    || sysTask.Status == TaskStatus.Faulted)
                //{
                //    var job = (JobBase)sysTask.AsyncState;
                //    if (job.HasRelatedProcess)
                //    {
                //        // 确保杀掉作业未释放的进程
                //        LogWriter.Write("prepare ensure kill job process");
                //        job.KillRelatedProcess();
                //    }
                //}
            }

            if (removeList.Count > 0)
            {
                foreach (var jobId in removeList)
                {
                    _activedJobs.TryRemove(jobId, out Task removeTask);
                    LogWriter.Write("已移除作业：" + jobId);
                }
            }
        }

        /// <summary>
        /// 检查激活数是否达到上限
        /// </summary>
        /// <returns><c>true</c>, if executing amount reach limit was checked, <c>false</c> otherwise.</returns>
        private bool CheckExecutingAmountReachLimit()
        {
            if (_activedJobs.Count >= _executeAmountLimit)
            {
                return true;
            }

            return false;
        }
    }
}
