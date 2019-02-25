using Swift.Core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Swift.DemoJob
{
    class Program
    {
        static void Main(string[] args)
        {
            // todo:将这个文件打包为可执行文件，其中使用反射访问用户编写的作业代码，用户只需要专心写业务代码，然后编译，和这个可执行文件打包到一起

            var paras = ResolveArguments(args);

            // 分割作业为不同的任务
            if (paras.ContainsKey("-d"))
            {
                var jobConfigPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "job.json");

                var jobConfigJson = File.ReadAllText(jobConfigPath, Encoding.UTF8);
                var jobWrapper = JobBase.Deserialize(jobConfigJson, null);
                var demoJob = jobWrapper.ConvertTo<DemoJob>();
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

                // 读取作业配置，创建当前作业的实例
                var jobConfigPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "job.json");
                var jobConfigJson = File.ReadAllText(jobConfigPath, Encoding.UTF8);
                var jobWrapper = JobBase.Deserialize(jobConfigJson, null);
                var demoJob = jobWrapper.ConvertTo<DemoJob>();

                // 读取任务配置，创建当前任务的实例
                var taskConfigPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "tasks", taskId, "task.json");
                var task = JobTask.CreateInstance(taskConfigPath);
                task.Job = jobWrapper;
                task.LoadRequirement();
                demoJob.PerformTask(task);
            }

            // 合并任务
            if (paras.ContainsKey("-m"))
            {
                // 读取作业配置，创建当前作业的实例
                var jobConfigPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "job.json");
                var jobConfigJson = File.ReadAllText(jobConfigPath, Encoding.UTF8);
                var jobWrapper = JobBase.Deserialize(jobConfigJson, null);
                var demoJob = jobWrapper.ConvertTo<DemoJob>();

                demoJob.CollectTaskResults();
            }
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
