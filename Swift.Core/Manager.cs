using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Swift.Core
{
    /// <summary>
    /// 经理
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
                var jobs = Cluster.Jobs;
                if (jobs.Count <= 0)
                {
                    WriteLog("没有作业真高兴...");
                    Thread.Sleep(10000);
                    continue;
                }

                // 尚未开始的作业
                var needProcessJobs = jobs.Where(d => d.Status == EnumJobRecordStatus.Pending);
                if (needProcessJobs.Any())
                {
                    foreach (var needProcessJob in needProcessJobs)
                    {
                        needProcessJob.CreateProductionPlan();
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
            WriteLog("经理开始干活了...");

            Cluster.MonitorJobConfigsFromDisk();
            Cluster.MonitorJobs();
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
