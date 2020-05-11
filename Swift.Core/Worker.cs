using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Swift.Core.Log;

namespace Swift.Core
{
    /// <summary>
    /// 工人
    /// </summary>
    public class Worker
    {
        private Member _member;
        private Cluster _cluster;
        private Thread _jobProcessThread;
        private CancellationTokenSource _jobProcessThreadCts;
        private Thread _cleanExitedSystemTaskThread;
        private CancellationTokenSource _cleanExitedSystemTaskThreadCts;
        private Thread _cleanOutOfControlChildProcessThread;
        private CancellationTokenSource _cleanOutOfControlChildProcessThreadCts;
        private readonly int _executeAmountLimit;
        private readonly ConcurrentDictionary<string, Task> _activedJobTasks = new ConcurrentDictionary<string, Task>();

        public Worker(Member member)
        {
            _member = member;
            _cluster = _member.Cluster;
            _executeAmountLimit = member.ConcurrentExecuteAmountLimit;
            _jobProcessThreadCts = new CancellationTokenSource();
        }

        /// <summary>
        /// 开始工作
        /// </summary>
        public void Start()
        {
            LogWriter.Write("worker play starting ...");

            CleanOutOfControlChildProcess();

            _cluster.MonitorJobConfigsFromConfigCenter();

            _jobProcessThreadCts = new CancellationTokenSource();
            StartProcessJobs(_jobProcessThreadCts.Token);

            _cleanExitedSystemTaskThreadCts = new CancellationTokenSource();
            StartCleanExitedSystemTask(_cleanExitedSystemTaskThreadCts.Token);

            _cleanOutOfControlChildProcessThreadCts = new CancellationTokenSource();
            StartCleanOutOfControlChildProcess(_cleanOutOfControlChildProcessThreadCts.Token);

            LogWriter.Write("worker play started");
        }

        /// <summary>
        /// 停止工作
        /// </summary>
        public void Stop()
        {
            LogWriter.Write("worker play stopping ...");

            _cluster.StopMonitorJobConfigsFromConfigCenter();

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
            LogWriter.Write("job process thread has exited");

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

            // 经过上边的处理，应该不会有未处理的进程了
            // KillChildProcess();

            LogWriter.Write("worker play stopped");
        }

        /// <summary>
        /// Kills the child process.
        /// </summary>
        private void KillChildProcess()
        {
            LogWriter.Write("begin close all child process: " + _activedJobTasks.Count);

            if (_activedJobTasks.Count > 0)
            {
                var taskIds = _activedJobTasks.Keys.ToArray();
                foreach (var taskId in taskIds)
                {
                    if (_activedJobTasks.TryRemove(taskId, out Task sysTask))
                    {
                        var jobTask = (JobTask)sysTask.AsyncState;
                        if (jobTask.Process != null && !jobTask.Process.HasExited)
                        {
                            LogWriter.Write("prepare kill task process: " + jobTask.BusinessId);
                            jobTask.KillProcess();
                        }

                        if (jobTask.Status != EnumTaskStatus.Pending)
                        {
                            jobTask.StartTime = DateTime.MinValue;
                            jobTask.UpdateTaskStatus(EnumTaskStatus.Pending);
                        }
                    }
                }
            }

            LogWriter.Write("end close all child process");
        }

        /// <summary>
        /// 启动处理作业
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

                    Thread.Sleep(3000);

                }
            })
            {
                Name = "JobProcessThread",
                IsBackground = true
            };

            _jobProcessThread.Start();
        }

        /// <summary>
        /// 启动清理退出的系统任务
        /// </summary>
        private void StartCleanExitedSystemTask(CancellationToken cancellationToken)
        {
            _cleanExitedSystemTaskThread = new Thread(() =>
            {
                while (true)
                {
                    // 这里没有传递cancellationToken，清理工作，不希望取消，尽量都执行
                    CleanExitedSystemTask();

                    // 放在清理后边执行就是为了尽可能的多清理一次
                    if (cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }

                    Thread.Sleep(3000);
                }
            })
            {
                Name = "CleanExitedSystemTaskThread",
                IsBackground = true
            };

            _cleanExitedSystemTaskThread.Start();
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

                    Thread.Sleep(3000);
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
            IEnumerable<string[]> taskExecuteProcess = processList.Where(d => d.GetValue(0).ToString() == "ExecuteTask");
            if (taskExecuteProcess.Any())
            {
                CleanOutOfControlTaskExecuteProcess(taskExecuteProcess);
            }
        }

        /// <summary>
        /// 清理任务合并进程
        /// </summary>
        /// <param name="collectTaskResultProcess">Collect task result process.</param>
        private void CleanOutOfControlCollectTaskResultProcess(IEnumerable<string[]> collectTaskResultProcess)
        {
            if (_member.Id != _cluster.Manager.Id)
            {
                LogWriter.Write("非Manager节点不应该执行任务合并进程，他们都应该被Kill");

                foreach (var processInfo in collectTaskResultProcess)
                {
                    var processId = int.Parse(processInfo[1]);
                    var jobName = processInfo[2];
                    var jobId = processInfo[3];

                    LogWriter.Write(string.Format("正在处理：{0},{1}", jobName, jobId));
                    SwiftProcess.KillAbandonedCollectTaskResultProcess(processId, jobName, jobId);
                }
            }
        }

        /// <summary>
        /// 清理脱离控制的作业分割进程
        /// </summary>
        /// <param name="jobSplitProcess">Job split process.</param>
        private void CleanOutOfControlJobSplitProcess(IEnumerable<string[]> jobSplitProcess)
        {
            if (_member.Id != _cluster.Manager.Id)
            {
                LogWriter.Write("非Manager节点不应该执行作业分割进程，他们都应该被Kill");

                foreach (var processInfo in jobSplitProcess)
                {
                    var processId = int.Parse(processInfo[1]);
                    var jobName = processInfo[2];
                    var jobId = processInfo[3];

                    LogWriter.Write(string.Format("正在处理：{0},{1}", jobName, jobId));
                    SwiftProcess.KillAbandonedJobSplitProcess(processId, jobName, jobId);
                }
            }
        }

        /// <summary>
        /// 清理脱离控制的执行任务进程
        /// </summary>
        /// <param name="taskExecuteProcess">Task execute process.</param>
        private void CleanOutOfControlTaskExecuteProcess(IEnumerable<string[]> taskExecuteProcess, CancellationToken cancellationToken = default)
        {
            foreach (var processInfo in taskExecuteProcess)
            {
                var processId = int.Parse(processInfo[1]);
                var jobName = processInfo[2];
                var jobId = processInfo[3];
                var taskId = int.Parse(processInfo[4]);

                LogWriter.Write(string.Format("正在处理：{0},{1},{2}", jobName, jobId, taskId), LogLevel.Debug);

                var jobRecord = _cluster.ConfigCenter.GetJobRecord(jobName, jobId, _cluster, cancellationToken);

                // 作业不存在了，看看任务进程还在不在
                if (jobRecord == null)
                {
                    LogWriter.Write(string.Format("作业记录不存在了，尝试关闭废弃的任务进程：{0},{1},{2}", jobName, jobId, taskId));
                    SwiftProcess.KillAbandonedTaskProcess(processId, jobName, jobId, taskId);
                    continue;
                }
                LogWriter.Write(string.Format("作业记录存在"), LogLevel.Debug);

                // 任务不是我的了，看看进程还在不在
                var task = jobRecord.TaskPlan.Where(d => d.Key == _member.Id && d.Value.Any(t => t.Id == taskId))
                                .SelectMany(d => d.Value).FirstOrDefault(t => t.Id == taskId);
                if (task == null)
                {
                    LogWriter.Write(string.Format("任务被重新分走了，尝试关闭废弃的任务进程：{0},{1},{2}", jobName, jobId, taskId));
                    SwiftProcess.KillAbandonedTaskProcess(processId, jobName, jobId, taskId);
                    continue;
                }
                LogWriter.Write(string.Format("任务存在"), LogLevel.Debug);

                // 任务非Executing状态，看看进程在不在
                if (task.Status != EnumTaskStatus.Executing)
                {
                    LogWriter.Write(string.Format("任务非Executing状态，尝试关闭废弃的任务进程：{0},{1},{2}", jobName, jobId, taskId));
                    SwiftProcess.KillAbandonedTaskProcess(processId, jobName, jobId, taskId);
                    continue;
                }
                LogWriter.Write(string.Format("任务在Executing状态，将继续运行"), LogLevel.Debug);

            }
        }

        /// <summary>
        /// 处理取消中的作业
        /// </summary>
        private void ProcessCancelingJobs(IEnumerable<JobTask> jobTasks)
        {
            foreach (var jobTask in jobTasks)
            {
                // 取消正在运行的任务
                if (_activedJobTasks.TryRemove(jobTask.BusinessId, out Task sysTask))
                {
                    var runningJobTask = (JobTask)sysTask.AsyncState;
                    if (runningJobTask.Process != null && !runningJobTask.Process.HasExited)
                    {
                        LogWriter.Write("prepare kill task process because job task is canceling");
                        runningJobTask.KillProcess();
                    }
                }

                LogWriter.Write("will update task status to Canceled: " + jobTask.BusinessId);
                jobTask.UpdateTaskStatus(EnumTaskStatus.Canceled);
            }
        }

        /// <summary>
        /// 处理任务执行失败的作业
        /// </summary>
        private void ProcessExecutingFailedJobs(IEnumerable<JobTask> jobTasks)
        {
            foreach (var jobTask in jobTasks)
            {
                // 取消正在运行的任务
                if (_activedJobTasks.TryRemove(jobTask.BusinessId, out Task sysTask))
                {
                    var runningJobTask = (JobTask)sysTask.AsyncState;
                    if (runningJobTask.Process != null && !runningJobTask.Process.HasExited)
                    {
                        LogWriter.Write("prepare kill task process because job is TaskExecutingFailed");
                        runningJobTask.KillProcess();
                    }
                }

                if (jobTask.Status == EnumTaskStatus.Executing)
                {
                    LogWriter.Write("will update task status to Canceled: " + jobTask.BusinessId);
                    jobTask.UpdateTaskStatus(EnumTaskStatus.Canceled);
                }
            }
        }

        /// <summary>
        /// 处理作业
        /// </summary>
        private void ProcessJobs(CancellationToken cancellationToken)
        {
            var jobs = _cluster.GetLatestJobRecords(cancellationToken);

            if (jobs == null || jobs.Length <= 0)
            {
                LogWriter.Write(string.Format("没有留任何作业，有点大轻松！"), Log.LogLevel.Trace);
                return;
            }

            // 计划执行完毕的作业可能包含当前节点的任务，则需要执行
            // 正在执行任务的作业可能包含当前节点的任务，则需要监控执行状态
            // 取消中的作业可能包含当前节点的任务，如果任务在运行，则需要取消
            // 任务执行失败的作业可能包含当前节点的任务，如果任务在运行，则需要取消
            var taskPlanCompletedJobs = jobs.Where(d =>
               d.Status == EnumJobRecordStatus.PlanMaked
            || d.Status == EnumJobRecordStatus.TaskExecuting
            || d.Status == EnumJobRecordStatus.Canceling
            || d.Status == EnumJobRecordStatus.TaskExecutingFailed);

            if (!taskPlanCompletedJobs.Any())
            {
                LogWriter.Write(string.Format("没有可执行作业，有点小轻松！"), Log.LogLevel.Trace);
                return;
            }

            // 遍历作业
            foreach (var job in taskPlanCompletedJobs)
            {
                ProcessJob(job, cancellationToken);
            }
        }

        /// <summary>
        /// 处理单个作业
        /// </summary>
        /// <param name="job">Job.</param>
        private void ProcessJob(JobBase job, CancellationToken cancellationToken)
        {
            LogWriter.Write(string.Format("开始处理作业：{0},{1}", job.Name, job.Id), LogLevel.Trace);

            // 确保作业包存在，否则去拉取
            _member.EnsureJobPackage(job.Name, job.Version, cancellationToken);

            if (job.TaskPlan.TryGetValue(_member.Id, out IEnumerable<JobTask> tasks))
            {
                LogWriter.Write(string.Format("发现当前成员的任务"), LogLevel.Debug);

                // 取消的任务需要先处理
                if (job.Status == EnumJobRecordStatus.Canceling)
                {
                    ProcessCancelingJobs(tasks);
                    return;
                }

                // 执行失败的作业需要先处理
                if (job.Status == EnumJobRecordStatus.TaskExecutingFailed)
                {
                    ProcessExecutingFailedJobs(tasks);
                    return;
                }

                // 确保作业记录空间存在
                EnsureJobSpace(job, cancellationToken);

                // 优先处理运行中状态的任务，因为这些任务的进程已经不受控制了，需要尽快处理
                IOrderedEnumerable<JobTask> orderedJobTasks = OrderTasksByStatus(tasks);

                foreach (var jobTask in orderedJobTasks)
                {
                    if (CheckExecutingAmountReachLimit())
                    {
                        LogWriter.Write("任务激活数已达上限。", LogLevel.Trace);
                        break;
                    }

                    ProcessJobTask(jobTask, cancellationToken);
                }
            }
        }

        /// <summary>
        /// 根据状态排序任务
        /// </summary>
        /// <returns>The tasks by status.</returns>
        /// <param name="tasks">Tasks.</param>
        private static IOrderedEnumerable<JobTask> OrderTasksByStatus(IEnumerable<JobTask> tasks)
        {
            return tasks.OrderBy(d => d.Status, Comparer<EnumTaskStatus>.Create((x, y) =>
            {
                if (x == EnumTaskStatus.Executing)
                {
                    return -1;
                }

                if (x == y)
                {
                    return 0;
                }

                return 1;
            }));
        }

        /// <summary>
        /// Processes the job task.
        /// </summary>
        /// <param name="task">Task.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private void ProcessJobTask(JobTask task, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_activedJobTasks.ContainsKey(task.BusinessId))
            {
                LogWriter.Write("任务循环检测到任务还在执行：" + task.BusinessId, Log.LogLevel.Debug);
                return;
            }

            // 运行中的任务，这种情况会发生在Swift进程被关闭的情况下，系统可能没重启也可能重启了
            if (task.Status == EnumTaskStatus.Executing)
            {
                LogWriter.Write("任务为运行中状态，将检查对应的进程：" + task.BusinessId, Log.LogLevel.Info);

                if (CheckExecutingJobTask(task, cancellationToken))
                {
                    _activedJobTasks.TryAdd(task.BusinessId, Task.Factory.StartNew(t => ((JobTask)t).MointorRun(cancellationToken), task, cancellationToken));
                    LogWriter.Write("任务循环已将任务激活：" + task.BusinessId, Log.LogLevel.Info);
                }
                return;
            }

            // 未处理的任务、处理失败的任务
            if (task.Status == EnumTaskStatus.Pending)
            {
                _activedJobTasks.TryAdd(task.BusinessId, Task.Factory.StartNew(t => ((JobTask)t).Run(cancellationToken), task, cancellationToken));
                LogWriter.Write("任务循环已将任务激活：" + task.BusinessId, Log.LogLevel.Info);
            }
        }

        /// <summary>
        /// 检查任务激活数是否达到上限
        /// </summary>
        /// <returns><c>true</c>, if executing amount reach limit was checked, <c>false</c> otherwise.</returns>
        private bool CheckExecutingAmountReachLimit()
        {
            if (_activedJobTasks.Count >= _executeAmountLimit)
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// 检查状态为执行中的任务，如果需要继续运行则返回true，否则返回false
        /// </summary>
        /// <returns><c>true</c>, if executing job task was checked, <c>false</c> otherwise.</returns>
        /// <param name="task">Task.</param>
        private bool CheckExecutingJobTask(JobTask task, CancellationToken cancellationToken = default)
        {
            int processId = task.GetProcessId(cancellationToken);

            // 没有进程Id，则认为任务在启动进程前就挂掉了或者进程没有启动成功
            if (processId == -1)
            {
                LogWriter.Write("没有进程Id，任务在启动进程前就挂掉了或者进程没有启动成功，将更新任务状态为准备");
                task.UpdateTaskStatus(EnumTaskStatus.Pending, cancellationToken);
                return false;
            }

            // 获取进程
            SwiftProcess.TryGetById(processId, task, out SwiftProcess process);

            // 如果任务执行状态文件标示已经执行完毕，则更新任务状态完成
            // 此时任务进程应该退出了，如果未退出可能是有未释放的资源，此时强行退出。
            var executeStatus = task.GetTaskExecuteStatus();
            if (executeStatus.ErrCode == 0)
            {
                LogWriter.Write("任务执行状态文件标示已经执行完毕，准备更新任务状态完成");

                task.UpdateTaskStatus(EnumTaskStatus.Completed, cancellationToken);

                if (process != null)
                {
                    task.KillProcess();
                }
                return false;
            }

            // 任务执行状态文件未标示完成，进程又没了
            if (process == null)
            {
                LogWriter.Write("找不到进程，将更新任务状态为准备");
                task.UpdateTaskStatus(EnumTaskStatus.Pending, cancellationToken);
                return false;
            }

            // 如果根据进程Id找到进程，则启动监控运行任务
            return true;
        }

        /// <summary>
        /// 清理退出的任务
        /// </summary>
        private void CleanExitedSystemTask()
        {
            List<string> removeList = new List<string>();

            foreach (var taskId in _activedJobTasks.Keys)
            {
                var task = _activedJobTasks[taskId];
                if (task.Status == TaskStatus.RanToCompletion)
                {
                    removeList.Add(taskId);
                    LogWriter.Write("prepare remove completed system task: " + taskId);
                }

                if (task.Status == TaskStatus.Canceled
                || task.Status == TaskStatus.Faulted)
                {
                    removeList.Add(taskId);
                    LogWriter.Write("prepare remove canceled or faulted system task: " + taskId);
                }

                if (task.Status == TaskStatus.RanToCompletion
                    || task.Status == TaskStatus.Canceled
                    || task.Status == TaskStatus.Faulted)
                {
                    var jobTask = (JobTask)task.AsyncState;
                    if (jobTask.Process != null && !jobTask.Process.HasExited)
                    {
                        // 脱离Swift控制的进程 可能会进入这里
                        LogWriter.Write("prepare kill task process because process has not exit");
                        jobTask.KillProcess();
                    }
                }
            }

            if (removeList.Count > 0)
            {
                foreach (var taskId in removeList)
                {
                    _activedJobTasks.TryRemove(taskId, out Task removeTask);
                    LogWriter.Write("task has removed: " + taskId);
                }
            }
        }

        /// <summary>
        /// 确保作业空间，如未创建则创建
        /// </summary>
        /// <param name="job">Job.</param>
        private static void EnsureJobSpace(JobBase job, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                if (!Directory.Exists(job.CurrentJobSpacePath))
                {
                    job.CreateJobSpace(cancellationToken);
                    LogWriter.Write(string.Format("已创建作业记录空间:{0}", job.Name));
                }
            }
            catch (Exception ex)
            {
                LogWriter.Write("执行任务前确保作业记录空间时发生异常", ex);
                throw;
            }
        }
    }
}
