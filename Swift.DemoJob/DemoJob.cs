using Newtonsoft.Json;
using Swift.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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

            return JsonConvert.SerializeObject(stat);
        }
    }
}
