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
            Cluster.MonitorJobConfigsFromConsul();
            Cluster.MonitorJobs();

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

            Download(Cluster.Manager, "download/job/package",
                string.Format("Jobs/{0}", jobConfig.Name + ".zip"));
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
                var jobs = Cluster.Jobs;
                if (jobs == null || jobs.Count <= 0)
                {
                    Thread.Sleep(5000);
                    continue;
                }

                var taskPlanCompletedJobs = jobs.Where(d => d.Status == EnumJobRecordStatus.PlanMaked || d.Status == EnumJobRecordStatus.TaskExecuting);
                if (!taskPlanCompletedJobs.Any())
                {
                    LogWriter.Write(string.Format("没有可执行的作业，有点小轻松！"));
                    Thread.Sleep(5000);
                    continue;
                }

                // TODO:处理作业的时候要不要锁住作业刷新，刷新出来的数据和处理后的数据可能不一致

                // 遍历已经制定计划完毕的作业，准备处理任务
                List<Task> jobList = new List<Task>();
                foreach (var job in taskPlanCompletedJobs)
                {
                    jobList.Add(Task.Factory.StartNew(() =>
                    {
                        // TODO:确保作业包存在，否则去拉取

                        // 判断作业记录目录是否存在
                        if (!Directory.Exists(job.CurrentJobSpacePath))
                        {
                            job.CreateJobSpace();
                            LogWriter.Write(string.Format("已创建作业记录所需文件:{0}", job.Name));
                        }

                        if (job.TaskPlan.ContainsKey(Id))
                        {
                            var tasks = job.TaskPlan[Id];
                            List<Task> taskList = new List<Task>();
                            foreach (var task in tasks)
                            {
                                // TODO:任务处理超时机制
                                if (task.Status == EnumTaskStatus.Pending) // 未处理的任务
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
