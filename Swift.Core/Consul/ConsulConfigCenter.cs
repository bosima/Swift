using System;
using System.Linq;
using System.Text;
using System.Threading;
using Consul;
using Newtonsoft.Json;
using Swift.Core.Log;

namespace Swift.Core.Consul
{
    public class ConsulConfigCenter : IConfigCenter
    {
        public ConsulConfigCenter()
        {
        }

        /// <summary>
        /// 获取作业记录在Consul KV中的全键名
        /// </summary>
        /// <returns>The job records full path.</returns>
        /// <param name="clusterName">Cluster name.</param>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        private string GetJobRecordFullKey(string clusterName, string jobName, string jobId)
        {
            string year = jobId.Substring(0, 4);
            string month = jobId.Substring(4, 2);
            string day = jobId.Substring(6, 2);
            return string.Format("Swift/{0}/Jobs/{1}/Records/{2}/{3}/{4}/{5}", clusterName, jobName, year, month, day, jobId);
        }

        /// <summary>
        /// 获取作业配置在Consul KV中的全键名
        /// </summary>
        /// <returns>The job config full key.</returns>
        /// <param name="clustername">Clustername.</param>
        /// <param name="jobName">Job name.</param>
        private string GetJobConfigFullKey(string clustername, string jobName)
        {
            return string.Format("Swift/{0}/Jobs/{1}/Config", clustername, jobName);
        }

        /// <summary>
        /// 更新作业状态
        /// </summary>
        /// <returns>The job status.</returns>
        /// <param name="job">Job.</param>
        /// <param name="status">Status.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public JobBase UpdateJobStatus(JobBase job, EnumJobRecordStatus status, CancellationToken cancellationToken = default(CancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            JobBase consulJobWrapper = null;
            var jobRecordKey = GetJobRecordFullKey(job.Cluster.Name, job.Name, job.Id);
            KVPair jobRecordKV;
            int updateTimes = 0;

            do
            {
                updateTimes++;
                Log.LogWriter.Write(string.Format("UpdateJobStatus Execute Times:{0},{1}", jobRecordKey, updateTimes), Log.LogLevel.Debug);

                if (updateTimes > 1)
                {
                    Thread.Sleep(200);
                }

                jobRecordKV = ConsulKV.Get(jobRecordKey, cancellationToken);
                var jobRecordJson = Encoding.UTF8.GetString(jobRecordKV.Value);
                Log.LogWriter.Write("UpdateJobStatus Get Value[" + jobRecordKV.ModifyIndex + "]" + jobRecordJson, Log.LogLevel.Trace);

                JobWrapper jobRecord = null;
                try
                {
                    jobRecord = JsonConvert.DeserializeObject<JobWrapper>(jobRecordJson);
                }
                catch (Exception ex)
                {
                    Log.LogWriter.Write(string.Format("反序列化作业异常: {0}", jobRecordKey), ex);
                    break;
                }

                if (jobRecord == null)
                {
                    Log.LogWriter.Write(string.Format("正在更新作业状态，但是作业已不存在: {0}", jobRecordKey), Log.LogLevel.Error);
                    break;
                }

                // 取消状态只能更新为 已取消或者取消失败
                if (jobRecord.Status == EnumJobRecordStatus.Canceling
                && status != EnumJobRecordStatus.Canceled && status != EnumJobRecordStatus.CancelFailed)
                {
                    return null;
                }

                jobRecord.ModifyIndex = jobRecordKV.ModifyIndex;
                jobRecord.Status = status;

                // 如果是任务计划还没有制定，则设置任务计划为null
                if (status == EnumJobRecordStatus.Pending)
                {
                    jobRecord.TaskPlan = null;
                    jobRecord.StartTime = DateTime.MinValue;
                    jobRecord.JobSplitStartTime = DateTime.MinValue;
                    jobRecord.CollectTaskResultStartTime = DateTime.MinValue;
                }
                // 如果是任务计划制定完毕，则附上计划
                if (status == EnumJobRecordStatus.PlanMaked)
                {
                    jobRecord.TaskPlan = job.TaskPlan;
                    jobRecord.JobSplitEndTime = DateTime.Now;
                }
                // 如果是任务计划开始制定，则附上开始时间
                if (status == EnumJobRecordStatus.PlanMaking)
                {
                    jobRecord.StartTime = DateTime.Now;
                    jobRecord.JobSplitStartTime = DateTime.Now;
                }
                // 如果是任务合并完毕，则附上结束时间
                if (status == EnumJobRecordStatus.TaskMerged)
                {
                    jobRecord.FinishedTime = DateTime.Now;
                    jobRecord.CollectTaskResultEndTime = DateTime.Now;
                }
                // 如果是任务结果合并，则附上开始时间
                if (status == EnumJobRecordStatus.TaskMerging)
                {
                    jobRecord.CollectTaskResultStartTime = DateTime.Now;
                }

                consulJobWrapper = jobRecord;

                try
                {
                    jobRecordJson = JsonConvert.SerializeObject(jobRecord);
                }
                catch (Exception ex)
                {
                    Log.LogWriter.Write(string.Format("序列化作业异常: {0}", jobRecordKey), ex);
                    break;
                }

                Log.LogWriter.Write("UpdateJobStatus CAS Value[" + jobRecordKV.ModifyIndex + "]" + jobRecordJson, Log.LogLevel.Trace);

                jobRecordKV.Value = Encoding.UTF8.GetBytes(jobRecordJson);
            } while (!ConsulKV.CAS(jobRecordKV, cancellationToken));

            return consulJobWrapper;
        }

        /// <summary>
        /// 更新任务状态
        /// </summary>
        /// <param name="status"></param>
        public JobBase UpdateTaskStatus(JobTask task, EnumTaskStatus status, CancellationToken cancellationToken = default(CancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            JobBase consulJobWrapper = null;
            var jobRecordKey = GetJobRecordFullKey(task.Job.Cluster.Name, task.Job.Name, task.Job.Id);
            KVPair jobRecordKV;
            int updateIndex = 0;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                updateIndex++;
                Log.LogWriter.Write(string.Format("UpdateTaskStatus Execute Times:{0},{1}", jobRecordKey + ":" + task.Id, updateIndex), Log.LogLevel.Debug);

                if (updateIndex > 1)
                {
                    Thread.Sleep(200);
                }

                jobRecordKV = ConsulKV.Get(jobRecordKey, cancellationToken);
                if (jobRecordKV == null)
                {
                    Log.LogWriter.Write(string.Format("UpdateTaskStatus->作业丢失{0}", jobRecordKey), Log.LogLevel.Error);
                    break;
                }

                var jobRecordJson = Encoding.UTF8.GetString(jobRecordKV.Value);
                Log.LogWriter.Write("UpdateTaskStatus Get Value[" + jobRecordKV.ModifyIndex + "]" + jobRecordJson, Log.LogLevel.Trace);

                var jobRecord = JobBase.Deserialize(jobRecordJson, task.Job.Cluster);
                jobRecord.ModifyIndex = jobRecordKV.ModifyIndex;

                var consulTask = jobRecord.TaskPlan.Where(d => d.Key == task.Job.Cluster.CurrentMember.Id).SelectMany(d => d.Value.Where(t => t.Id == task.Id)).FirstOrDefault();
                if (status == EnumTaskStatus.Synced
                || status == EnumTaskStatus.SyncFailed)
                {
                    consulTask = jobRecord.TaskPlan.SelectMany(d => d.Value.Where(t => t.Id == task.Id)).FirstOrDefault();
                }

                if (consulTask == null)
                {
                    Log.LogWriter.Write(string.Format("UpdateTaskStatus->任务丢失{0}", jobRecordKey), Log.LogLevel.Error);
                    break;
                }

                // 取消状态只能更新为 已取消或者取消失败
                if (consulTask.Status == EnumTaskStatus.Canceling
                && status != EnumTaskStatus.Canceled && status != EnumTaskStatus.CancelFailed)
                {
                    return null;
                }

                // TODO:增加一些状态判断，只有在某个状态才能变为某个状态，防止多线程情况下状态被回退

                consulTask.Status = status;
                if (status == EnumTaskStatus.Executing)
                {
                    consulTask.StartTime = DateTime.Now;
                }
                if (status == EnumTaskStatus.Completed)
                {
                    consulTask.FinishedTime = DateTime.Now;
                }

                consulJobWrapper = jobRecord;

                jobRecordJson = JsonConvert.SerializeObject(jobRecord);
                Log.LogWriter.Write("UpdateTaskStatus CAS Value[" + jobRecordKV.ModifyIndex + "]" + jobRecordJson, Log.LogLevel.Trace);

                jobRecordKV.Value = Encoding.UTF8.GetBytes(jobRecordJson);

            } while (!ConsulKV.CAS(jobRecordKV, cancellationToken));

            return consulJobWrapper;
        }

        /// <summary>
        /// 更新作业任务计划
        /// </summary>
        /// <returns>The job task plan.</returns>
        /// <param name="job">Job.</param>
        public JobBase UpdateJobTaskPlan(JobBase job, CancellationToken cancellationToken = default(CancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            JobBase consulJobWrapper = null;
            KVPair jobRecordKV;
            int updateTimes = 0;

            do
            {
                updateTimes++;
                Log.LogWriter.Write("UpdateJobTaskPlan Execute Times:" + updateTimes, Log.LogLevel.Info);

                if (updateTimes > 1)
                {
                    Thread.Sleep(200);
                }

                var jobRecordKey = GetJobRecordFullKey(job.Cluster.Name, job.Name, job.Id);
                jobRecordKV = ConsulKV.Get(jobRecordKey, cancellationToken);
                var jobRecordJson = Encoding.UTF8.GetString(jobRecordKV.Value);
                var jobRecord = JsonConvert.DeserializeObject<JobWrapper>(jobRecordJson);
                if (jobRecord == null)
                {
                    Log.LogWriter.Write(string.Format("正在更新作业任务计划，但是作业已不存在: {0}", jobRecordKey), Log.LogLevel.Error);
                    break;
                }

                jobRecord.ModifyIndex = jobRecordKV.ModifyIndex;
                jobRecord.TaskPlan = job.TaskPlan;

                if (jobRecord.Status == EnumJobRecordStatus.TaskCompleted)
                {
                    jobRecord.Status = EnumJobRecordStatus.TaskExecuting;
                }

                // 将作业信息更新到本地
                consulJobWrapper = jobRecord;

                jobRecordJson = JsonConvert.SerializeObject(jobRecord);
                jobRecordKV.Value = Encoding.UTF8.GetBytes(jobRecordJson);
            } while (!ConsulKV.CAS(jobRecordKV, cancellationToken)); // 可能同时更新作业记录配置，所以这里用CAS

            return consulJobWrapper;
        }

        /// <summary>
        /// 更新最后一次作业记录
        /// </summary>
        /// <param name="newJob">New job.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public bool UpdateLastJobRecord(JobBase newJob, CancellationToken cancellationToken = default(CancellationToken))
        {
            // TODO:可能在这里中断，需要搞个类似事务的机制，比如先写一个新记录标记，然后写作业记录，然后更新作业配置，最后删除记录文件
            // 如果创建新纪录的时候，新纪录标记存在，则检查作业记录和作业配置是否是最新的

            var newJobKey = GetJobRecordFullKey(newJob.Cluster.Name, newJob.Name, newJob.Id);
            var newJobKV = ConsulKV.Create(newJobKey);
            newJobKV.Value = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(newJob));
            bool createResult = ConsulKV.CAS(newJobKV, cancellationToken);
            LogWriter.Write(string.Format("create job record kv result: {0},{1},{2}", newJob.Name, newJob.Id, createResult));
            if (!createResult)
            {
                return false;
            }

            KVPair jobConfigKV = null;
            int doTimes = 0;
            do
            {
                doTimes++;
                LogWriter.Write(string.Format("update the last job record id of the job config in config center: {0}", doTimes), Log.LogLevel.Debug);

                var jobConfigKey = GetJobConfigFullKey(newJob.Cluster.Name, newJob.Name);
                jobConfigKV = ConsulKV.Get(jobConfigKey, cancellationToken);
                if (jobConfigKV == null)
                {
                    LogWriter.Write(string.Format("the job config not exists in config center: {0}", newJob.Name));
                    return false;
                }

                var jobConfig = JobConfig.CreateInstance(Encoding.UTF8.GetString(jobConfigKV.Value));
                jobConfig.LastRecordId = newJob.Id;
                jobConfig.LastRecordCreateTime = DateTime.Now;
                jobConfig.ModifyIndex = jobConfigKV.ModifyIndex;

                var jobConfigJson = JsonConvert.SerializeObject(jobConfig);
                jobConfigKV.Value = Encoding.UTF8.GetBytes(jobConfigJson);
            }
            while (!ConsulKV.CAS(jobConfigKV, cancellationToken));

            LogWriter.Write(string.Format("the last job record id of the job configs has updated: {0}", newJob.Name));
            return true;
        }

        /// <summary>
        /// 获取作业最新记录
        /// </summary>
        /// <returns>The job record.</returns>
        /// <param name="jobConfig">Job config.</param>
        /// <param name="cluster">Cluster.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public JobBase GetJobRecord(JobConfig jobConfig, Cluster cluster, CancellationToken cancellationToken)
        {
            return GetJobRecord(jobConfig.Name, jobConfig.LastRecordId, cluster, cancellationToken);
        }

        /// <summary>
        /// Gets the job record.
        /// </summary>
        /// <returns>The job record.</returns>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        /// <param name="cluster">Cluster.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public JobBase GetJobRecord(string jobName, string jobId, Cluster cluster, CancellationToken cancellationToken)
        {
            var jobRecordKey = GetJobRecordFullKey(cluster.Name, jobName, jobId);
            var jobRecordKV = ConsulKV.Get(jobRecordKey, cancellationToken);
            if (jobRecordKV == null || jobRecordKV.Value == null)
            {
                LogWriter.Write(string.Format("配置中心作业记录丢失[{0}->{1}]", jobName, jobId), Log.LogLevel.Error);
                return null;
            }

            var jobRecord = JobBase.Deserialize(Encoding.UTF8.GetString(jobRecordKV.Value), cluster);
            jobRecord.ModifyIndex = jobRecordKV.ModifyIndex;

            return jobRecord;
        }

        /// <summary>
        /// Cancels the job record.
        /// </summary>
        /// <returns><c>true</c>, if job record was canceled, <c>false</c> otherwise.</returns>
        /// <param name="jobRecord">Job record.</param>
        public bool CancelJobRecord(JobBase jobRecord)
        {
            var jobRecordKey = GetJobRecordFullKey(jobRecord.Cluster.Name, jobRecord.Name, jobRecord.Id);
            var jobRecordKV = ConsulKV.Get(jobRecordKey);
            if (jobRecordKV == null || jobRecordKV.Value == null)
            {
                LogWriter.Write(string.Format("配置中心作业记录丢失：{0}", jobRecord.BusinessId), Log.LogLevel.Warn);
                return true;
            }

            if (jobRecordKV.ModifyIndex != jobRecord.ModifyIndex)
            {
                return false;
            }

            jobRecordKV.Value = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(jobRecord));
            return ConsulKV.CAS(jobRecordKV);
        }
    }
}
