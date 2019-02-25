using Consul;
using Newtonsoft.Json;
using Swift.Core.Consul;
using Swift.Core.Log;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;

namespace Swift.Core
{
    /// <summary>
    /// 工人
    /// </summary>
    public class Worker : Member
    {
        private Thread taskProcessThread;

        public Worker()
        {
            this.Role = EnumMemberRole.Worker;
        }

        /// <summary>
        /// 开始工作
        /// </summary>
        protected override void Start()
        {
            LogWriter.Write("工人开始干活了...");
            Cluster.OnJobConfigJoinEventHandler += Cluster_OnJobConfigJoinEventHandler;
            Cluster.OnJobConfigUpdateEventHandler += Cluster_OnJobConfigUpdateEventHandler;
            Cluster.OnJobConfigRemoveEventHandler += Cluster_OnJobConfigRemoveEventHandler;

            Cluster.MonitorMembers();
            Cluster.MonitorJobConfigsFromConsul();
            //Cluster.MonitorJobs();

            StartProcessTasks();
        }

        /// <summary>
        /// 作业配置更新处理
        /// </summary>
        /// <param name="jobConfig"></param>
        private void Cluster_OnJobConfigUpdateEventHandler(JobConfig jobConfig)
        {
            // 下载作业配置
            if (Cluster.Manager == null)
            {
                LogWriter.Write(string.Format("集群[{0}]不存在Manager，无法下载作业包", Cluster.Name));
                return;
            }

            Download(Cluster.Manager, "download/job/package",
                string.Format("Jobs/{0}", jobConfig.Name + ".zip"));
        }

        /// <summary>
        /// 作业配置移除处理
        /// </summary>
        /// <param name="jobConfig"></param>
        private void Cluster_OnJobConfigRemoveEventHandler(JobConfig jobConfig)
        {
            jobConfig.RemoveAllFile();
        }

        /// <summary>
        /// 作业配置加入处理
        /// </summary>
        /// <param name="jobConfig"></param>
        private void Cluster_OnJobConfigJoinEventHandler(JobConfig jobConfig)
        {
            // 下载作业配置
            if (Cluster.Manager == null)
            {
                LogWriter.Write(string.Format("集群[{0}]不存在Manager，无法下载作业包", Cluster.Name));
                return;
            }

            DownloadJobPackage(jobConfig.Name);
        }

        private void DownloadJobPackage(string jobConfigName)
        {
            Download(Cluster.Manager, "download/job/package",
               string.Format("Jobs/{0}", jobConfigName + ".zip"));
        }

        private void EnsureJobPackage(string jobConfigName)
        {
            var jobPkgPath = Path.Combine(SwiftConfiguration.BaseDirectory, "Jobs", jobConfigName + ".zip");
            if (!File.Exists(jobPkgPath))
            {
                DownloadJobPackage(jobConfigName);
            }
        }

        private void StartProcessTasks()
        {
            taskProcessThread = new Thread(ProcessTasks)
            {
                Name = "taskProcessThread",
                IsBackground = true
            };

            taskProcessThread.Start();
        }

        /// <summary>
        /// 处理任务
        /// </summary>
        private void ProcessTasks()
        {
            while (true)
            {
                var jobs = Cluster.GetCurrentJobs();
                if (jobs == null || jobs.Length <= 0)
                {
                    Thread.Sleep(5000);
                    continue;
                }

                var taskPlanCompletedJobs = jobs.Where(d => d.Status == EnumJobRecordStatus.PlanMaked
                || d.Status == EnumJobRecordStatus.TaskExecuting);
                if (!taskPlanCompletedJobs.Any())
                {
                    LogWriter.Write(string.Format("没有可执行的作业，有点小轻松！"));
                    Thread.Sleep(5000);
                    continue;
                }

                // 遍历已经制定计划完毕的作业，准备处理任务
                List<Task> jobList = new List<Task>();
                foreach (var job in taskPlanCompletedJobs)
                {
                    jobList.Add(Task.Factory.StartNew(() =>
                    {
                        try
                        {
                            // TODO:获取最新的作业包
                            // 确保作业包存在，否则去拉取：可能发现作业配置的时候因为各种原因没有拉取成功
                            EnsureJobPackage(job.Name);
                        }
                        catch (Exception ex)
                        {
                            LogWriter.Write("执行任务前确保作业包存在时发生异常", ex);
                            return;
                        }

                        // 判断作业记录目录是否存在
                        if (!Directory.Exists(job.CurrentJobSpacePath))
                        {
                            job.CreateJobSpace();
                            LogWriter.Write(string.Format("已创建作业记录所需文件:{0}", job.Name));
                        }

                        if (job.TaskPlan.ContainsKey(Id))
                        {
                            // TODO:控制任务并行数量为CPU逻辑处理器数量

                            var tasks = job.TaskPlan[Id];
                            List<Task> taskList = new List<Task>();
                            foreach (var task in tasks)
                            {
                                // 未处理的任务、处理失败的任务、正在执行的任务（执行被中断）
                                if (task.Status == EnumTaskStatus.Pending
                                || task.Status == EnumTaskStatus.Failed
                                || task.Status == EnumTaskStatus.Executing)
                                {
                                    LogWriter.Write(string.Format("发现未处理任务:{0},{1},{2}", job.Name, job.Id, task.Id));

                                    taskList.Add(Task.Factory.StartNew(() =>
                                    {
                                        job.RunTask(task);
                                    }));
                                }
                            }
                            Task.WaitAll(taskList.ToArray());
                        }

                    }));
                }

                Task.WaitAll(jobList.ToArray());

                Thread.Sleep(3000);
            }
        }

        /// <summary>
        /// 停止工作
        /// </summary>
        protected override void Stop()
        {
            if (taskProcessThread != null)
            {
                taskProcessThread.Abort();
            }
        }
    }
}
