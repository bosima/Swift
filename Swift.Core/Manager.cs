using System.Linq;
using System.Threading;
using Swift.Core.Log;

namespace Swift.Core
{
    /// <summary>
    /// 管理员
    /// </summary>
    public class Manager : Member
    {
        private Thread jobProcessThread;

        /// <summary>
        /// 构造函数
        /// </summary>
        public Manager()
        {
            this.Role = EnumMemberRole.Manager;
        }

        /// <summary>
        /// 开启处理作业
        /// </summary>
        private void StartProcessJobs()
        {
            jobProcessThread = new Thread(ProcessJobs)
            {
                Name = "jobProcessThread",
                IsBackground = true
            };

            jobProcessThread.Start();
        }

        /// <summary>
        /// 处理作业
        /// </summary>
        private void ProcessJobs()
        {
            while (true)
            {
                var jobs = Cluster.GetCurrentJobs();
                if (jobs.Length <= 0)
                {
                    LogWriter.Write("没有作业真高兴...");
                    Thread.Sleep(5000);
                    continue;
                }

                var workers = Cluster.GetCurrentWorkers();
                if (workers == null || !workers.Any(d => d.Status == 1))
                {
                    LogWriter.Write("没有在线的工人，光杆司令没法干活...");
                    Thread.Sleep(5000);
                    continue;
                }

                // 需要开始处理的作业:待处理、计划指定失败、正在制定计划（不应该存在这种状态，除非异常中断）
                var needStartJobs = jobs.Where(d => d.Status == EnumJobRecordStatus.Pending
                || d.Status == EnumJobRecordStatus.PlanFailed
                || d.Status == EnumJobRecordStatus.PlanMaking);
                if (needStartJobs.Any())
                {
                    foreach (var needStartJob in needStartJobs)
                    {
                        needStartJob.CreateProductionPlan();
                    }
                }

                // 正在执行任务的作业
                var taskProcessingJobs = jobs.Where(d => d.Status == EnumJobRecordStatus.TaskExecuting);
                if (taskProcessingJobs.Any())
                {
                    foreach (var taskProcessingJob in taskProcessingJobs)
                    {
                        taskProcessingJob.SyncTaskResult();
                    }
                }

                // 任务正在执行或都处理完成的作业
                var taskProcessingAndCompletedJobs = jobs.Where(d => d.Status == EnumJobRecordStatus.TaskExecuting || d.Status == EnumJobRecordStatus.TaskCompleted);
                if (taskProcessingAndCompletedJobs.Any())
                {
                    foreach (var job in taskProcessingAndCompletedJobs)
                    {
                        job.CheckTaskRunStatus();
                    }
                }

                // TODO:处理已经不存在的节点，重新分配任务;是否给新增的节点加点任务？

                // 合并任务同步完成的作业
                var taskSyncedJobs = jobs.Where(d => d.Status == EnumJobRecordStatus.TaskSynced);
                if (taskSyncedJobs.Any())
                {
                    foreach (var taskSyncedJob in taskSyncedJobs)
                    {
                        taskSyncedJob.MergeTaskResult();
                    }
                }

                Thread.Sleep(2000);
            }
        }

        /// <summary>
        /// 开始工作
        /// </summary>
        protected override void Start()
        {
            LogWriter.Write("Manager开始干活了...");

            Cluster.MonitorMembers();
            Cluster.MonitorMembersHealth();
            Cluster.MonitorJobConfigsFromDisk();
            Cluster.MonitorJobConfigsFromConsul();
            //Cluster.MonitorJobs();
            Cluster.MonitorJobCreate();
            StartProcessJobs();
        }

        /// <summary>
        /// 停止工作
        /// </summary>
        protected override void Stop()
        {
            if (jobProcessThread != null)
            {
                jobProcessThread.Abort();
            }
        }
    }
}
