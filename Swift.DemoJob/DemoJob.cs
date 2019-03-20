using Newtonsoft.Json;
using Swift.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Swift.DemoJob
{
    public class DemoJob : JobBase
    {
        /// <summary>
        /// 将工作分成若干任务
        /// </summary>
        public override JobTask[] Split()
        {
            string input = "With more than 35,000 production deployments of RabbitMQ world-wide at small startups and large enterprises, RabbitMQ is the most popular open source message broker.";

            int taskNum = 4;
            int perTaskCharNos = (int)Math.Ceiling(input.Length / (double)taskNum);

            List<JobTask> taskList = new List<JobTask>();
            int k = 1;
            for (int i = 0; i < taskNum; i++)
            {
                if (i * perTaskCharNos < input.Length - 1)
                {
                    var subLength = perTaskCharNos;
                    if (i * perTaskCharNos + subLength > input.Length)
                    {
                        subLength = input.Length - i * perTaskCharNos;
                    }

                    // 需求只需要时字符串，格式自己定义
                    string requirement = input.Substring(i * perTaskCharNos, subLength);
                    var task = new JobTask()
                    {
                        Id = k,
                        Job = new JobWrapper(this),
                        Requirement = requirement,
                    };
                    taskList.Add(task);
                    k++;
                }
            }

            return taskList.ToArray();
        }

        /// <summary>
        /// 执行任务
        /// </summary>
        /// <returns></returns>
        public override string ExecuteTask(JobTask task)
        {
            Dictionary<char, int> stat = new Dictionary<char, int>();

            // 使用自己定义的需求
            var requirement = task.Requirement;
            if (requirement != null)
            {
                foreach (var c in requirement)
                {
                    if (stat.ContainsKey(c))
                    {
                        stat[c]++;
                    }
                    else
                    {
                        stat.Add(c, 1);
                    }
                }
            }

            // 测试超时的处理
            Thread.Sleep(1 * 60 * 1000);

            // 如果任务结果文件比较大，比如300M，会导致网速和内存占用较大，请考虑写到别的地方去，这里返回个实际结果的地址就行了
            // 返回字符串的格式自定义即可
            return JsonConvert.SerializeObject(stat);
        }

        /// <summary>
        /// 汇集各个任务的处理结果
        /// </summary>
        /// <returns></returns>
        public override string Collect(IEnumerable<JobTask> tasks)
        {
            Dictionary<char, int> stat = new Dictionary<char, int>();

            foreach (var task in tasks)
            {
                // 自定义的任务结果字符串
                var taskResult = task.Result;
                if (!string.IsNullOrWhiteSpace(taskResult))
                {
                    var taskStat = JsonConvert.DeserializeObject<Dictionary<char, int>>(taskResult);

                    foreach (var c in taskStat.Keys)
                    {
                        if (stat.ContainsKey(c))
                        {
                            stat[c] += taskStat[c];
                        }
                        else
                        {
                            stat[c] = taskStat[c];
                        }
                    }
                }
            }

            // 如果结果太大，比如超过1G，写文件可能会很慢，请考虑写到别的地方去，这里返回个实际结果的地址就行了
            // 返回字符串的格式自定义即可
            return JsonConvert.SerializeObject(stat);
        }
    }
}
