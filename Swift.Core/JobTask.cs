using Consul;
using Newtonsoft.Json;
using Swift.Core.Consul;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Swift.Core
{
    /// <summary>
    /// 任务类
    /// </summary>
    public class JobTask
    {
        /// <summary>
        /// 任务Id
        /// </summary>
        public int Id
        {
            get;
            set;
        }

        /// <summary>
        /// 所属作业
        /// </summary>
        [JsonIgnore]
        public JobWrapper Job
        {
            get;
            set;
        }

        /// <summary>
        /// 任务需求
        /// </summary>
        [JsonIgnore]
        public string Requirement
        {
            get;
            set;
        }

        /// <summary>
        /// 任务状态
        /// </summary>
        public EnumTaskStatus Status
        {
            get;
            set;
        }

        /// <summary>
        /// 任务处理结果
        /// </summary>
        [JsonIgnore]
        public string Result
        {
            get;
            set;
        }

        /// <summary>
        /// 当前任务所在路径
        /// </summary>
        [JsonIgnore]
        public string CurrentTaskPath
        {
            get
            {
                return Path.Combine(Job.CurrentJobSpacePath, "tasks", Id.ToString());
            }
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        public JobTask()
        {
        }

        /// <summary>
        /// 根据任务配置文件创建任务实例
        /// </summary>
        /// <param name="physicalConfigPath"></param>
        /// <returns></returns>
        public static JobTask CreateInstance(string physicalConfigPath)
        {
            if (!File.Exists(physicalConfigPath))
            {
                throw new Exception(string.Format("任务配置文件不存在:{0}", physicalConfigPath));
            }

            var taskConfigJson = string.Empty;
            try
            {
                taskConfigJson = File.ReadAllText(physicalConfigPath, Encoding.UTF8);
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("读取任务配置文件异常:{0}", ex.Message));
            }

            if (string.IsNullOrWhiteSpace(taskConfigJson))
            {
                throw new Exception(string.Format("任务配置文件为空:{0}", physicalConfigPath));
            }

            return JsonConvert.DeserializeObject<JobTask>(taskConfigJson);
        }

        /// <summary>
        /// 更新任务状态
        /// </summary>
        /// <param name="status"></param>
        public void UpdateTaskStatus(EnumTaskStatus status)
        {
            Status = status;

            var jobRecordKey = string.Format("Swift/{0}/Jobs/{1}/Records/{2}", Job.Cluster.Name, Job.Name, Job.Id);
            KVPair jobRecordKV;
            int updateIndex = 0;
            do
            {
                updateIndex++;
                Log.LogWriter.Write("UpdateTaskStatus Execute Index:" + updateIndex, Log.LogLevel.Info);

                Thread.Sleep(1000);

                jobRecordKV = ConsulKV.Get(jobRecordKey);
                if (jobRecordKV == null)
                {
                    Log.LogWriter.Write(string.Format("更新任务状态时找不到作业是什么鬼？{0}", jobRecordKey), Log.LogLevel.Error);
                    break;
                }

                var jobRecord = JobBase.Deserialize(Encoding.UTF8.GetString(jobRecordKV.Value), Job.Cluster);
                var consulTask = jobRecord.TaskPlan.SelectMany(d => d.Value.Where(t => t.Id == Id)).FirstOrDefault();
                if (consulTask == null)
                {
                    Log.LogWriter.Write(string.Format("更新任务状态时找不到任务是什么鬼？{0}", jobRecordKey), Log.LogLevel.Error);
                    break;
                }

                consulTask.Status = Status;
                var jobRecordJson = JsonConvert.SerializeObject(jobRecord);
                jobRecordKV.Value = Encoding.UTF8.GetBytes(jobRecordJson);

                Log.LogWriter.Write("UpdateTaskStatus Value[" + jobRecordKV.ModifyIndex + "]" + jobRecordJson, Log.LogLevel.Trace);

            } while (!ConsulKV.CAS(jobRecordKV));
        }

        /// <summary>
        /// 加载结果
        /// </summary>
        public void LoadResult()
        {
            var physicalResultPath = Path.Combine(CurrentTaskPath, "result.txt");

            if (!File.Exists(physicalResultPath))
            {
                throw new Exception(string.Format("任务结果文件不存在:{0}", physicalResultPath));
            }

            var taskResultJson = string.Empty;
            try
            {
                taskResultJson = File.ReadAllText(physicalResultPath, Encoding.UTF8);
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("读取任务结果文件异常:{0}", ex.Message));
            }

            if (string.IsNullOrWhiteSpace(taskResultJson))
            {
                throw new Exception(string.Format("任务结果文件为空:{0}", physicalResultPath));
            }

            Result = taskResultJson;
        }

        /// <summary>
        /// 加载需求
        /// </summary>
        public void LoadRequirement()
        {
            var physicalRequirementPath = Path.Combine(CurrentTaskPath, "requirement.txt");

            if (!File.Exists(physicalRequirementPath))
            {
                throw new Exception(string.Format("任务需求文件不存在:{0}", physicalRequirementPath));
            }

            var taskRequirementJson = string.Empty;
            try
            {
                taskRequirementJson = File.ReadAllText(physicalRequirementPath, Encoding.UTF8);
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("读取任务需求文件异常:{0}", ex.Message));
            }

            if (string.IsNullOrWhiteSpace(taskRequirementJson))
            {
                throw new Exception(string.Format("任务需求文件为空:{0}", physicalRequirementPath));
            }

            Requirement = taskRequirementJson;
        }

        /// <summary>
        /// 将任务配置写到文件
        /// </summary>
        public void WriteConfig()
        {
            var taskPath = CurrentTaskPath;
            if (!Directory.Exists(taskPath))
            {
                Directory.CreateDirectory(taskPath);
            }

            var taskJson = JsonConvert.SerializeObject(this);
            string taskJsonPath = Path.Combine(taskPath, "task.json");
            File.WriteAllText(taskJsonPath, taskJson, Encoding.UTF8);
        }

        /// <summary>
        /// 将任务需求写到文件
        /// </summary>
        public void WriteRequirement()
        {
            var taskPath = CurrentTaskPath;
            if (!Directory.Exists(taskPath))
            {
                Directory.CreateDirectory(taskPath);
            }

            string taskRequirementPath = Path.Combine(taskPath, "requirement.txt");
            File.WriteAllText(taskRequirementPath, Requirement, Encoding.UTF8);
        }

        /// <summary>
        /// 保存任务结果
        /// </summary>
        /// <param name="result"></param>
        public void WriteResult()
        {
            var taskPath = CurrentTaskPath;
            if (!Directory.Exists(taskPath))
            {
                Directory.CreateDirectory(taskPath);
            }

            string taskRequirementPath = Path.Combine(taskPath, "result.txt");
            File.WriteAllText(taskRequirementPath, Result, Encoding.UTF8);
        }

    }
}
