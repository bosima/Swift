using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Swift.Core;

namespace Swift.JobEntryPoint
{
    class Program
    {
        static void Main(string[] args)
        {
            var paras = ResolveArguments(args);

            // 分割作业为不同的任务
            if (paras.ContainsKey("-d"))
            {
                var jobSpacePath = paras["-jp"];
                var jobConfigPath = Path.Combine(jobSpacePath, "job.json");
                var jobConfigJson = File.ReadAllText(jobConfigPath, Encoding.UTF8);
                var jobWrapper = JobBase.Deserialize(jobConfigJson, null);
                var demoJob = CreateJobInstance(jobWrapper.JobClassName, jobWrapper);
                demoJob.GenerateTasks();
            }

            // 处理任务
            if (paras.ContainsKey("-p"))
            {
                if (!paras.ContainsKey("-t"))
                {
                    Console.Write("缺少任务Id");
                    return;
                }

                var taskId = paras["-t"];
                var jobSpacePath = paras["-jp"];

                // 读取作业配置，创建当前作业的实例
                var jobConfigPath = Path.Combine(jobSpacePath, "job.json");
                var jobConfigJson = File.ReadAllText(jobConfigPath, Encoding.UTF8);
                var jobWrapper = JobBase.Deserialize(jobConfigJson, null);
                var demoJob = CreateJobInstance(jobWrapper.JobClassName, jobWrapper);

                // 读取任务配置，创建当前任务的实例
                var taskConfigPath = Path.Combine(jobSpacePath, "tasks", taskId, "task.json");
                var task = JobTask.CreateInstance(taskConfigPath);
                task.Job = jobWrapper;
                task.LoadRequirement();
                demoJob.PerformTask(task);
            }

            // 合并任务
            if (paras.ContainsKey("-m"))
            {
                // 读取作业配置，创建当前作业的实例
                var jobSpacePath = paras["-jp"];
                var jobConfigPath = Path.Combine(jobSpacePath, "job.json");
                var jobConfigJson = File.ReadAllText(jobConfigPath, Encoding.UTF8);
                var jobWrapper = JobBase.Deserialize(jobConfigJson, null);
                var demoJob = CreateJobInstance(jobWrapper.JobClassName, jobWrapper);

                demoJob.CollectTaskResults();
            }
        }

        private static JobBase CreateJobInstance(string jobClassInfo, JobWrapper jobWrapper)
        {
            var jobClass = jobClassInfo.Split(',');
            var jobClassFile = Path.Combine(jobWrapper.CurrentJobProgramPath, jobClass[0]);
            var jobClassName = jobClass[1];
            Assembly assembly = System.Reflection.Assembly.LoadFrom(jobClassFile);
            Type type = assembly.GetType(jobClassName);
            object obj = Activator.CreateInstance(type, true);
            var job = (JobBase)obj;
            job.CopyMetaFrom(jobWrapper);
            return job;
        }

        /// <summary>
        /// 解析启动参数
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        private static Dictionary<string, string> ResolveArguments(string[] args)
        {
            Dictionary<string, string> paras = new Dictionary<string, string>();

            for (int i = 0; i < args.Length; i++)
            {
                if (!args[i].StartsWith("-", StringComparison.Ordinal))
                {
                    continue;
                }

                var key = args[i].ToLower();

                if (string.IsNullOrWhiteSpace(key))
                {
                    continue;
                }

                var val = string.Empty;
                if (i + 1 < args.Length)
                {
                    if (!args[i + 1].StartsWith("-", StringComparison.Ordinal))
                    {
                        val = args[i + 1].Trim();
                        i = i + 1;
                    }
                }

                paras.Add(key, val);
            }

            return paras;
        }
    }
}
