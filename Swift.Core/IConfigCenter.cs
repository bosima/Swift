using System;
using System.Threading;

namespace Swift.Core
{
    public interface IConfigCenter
    {
        /// <summary>
        /// 更新作业实例状态
        /// </summary>
        /// <returns>配置中心的作业实例信息</returns>
        /// <param name="job">Job.</param>
        /// <param name="status">Status.</param>
        JobBase UpdateJobStatus(JobBase job, EnumJobRecordStatus status, CancellationToken cancellationToken);

        /// <summary>
        /// 更新任务状态
        /// </summary>
        /// <returns>The task status.</returns>
        /// <param name="task">Task.</param>
        /// <param name="status">Status.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        JobBase UpdateTaskStatus(JobTask task, EnumTaskStatus status, CancellationToken cancellationToken);

        /// <summary>
        /// 更新作业任务计划
        /// </summary>
        /// <returns>The job task plan.</returns>
        /// <param name="job">Job.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        JobBase UpdateJobTaskPlan(JobBase job, CancellationToken cancellationToken);

        /// <summary>
        /// 更新最后一次作业记录
        /// </summary>
        /// <param name="newJob">Job base.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        bool UpdateLastJobRecord(JobBase newJob, CancellationToken cancellationToken);

        /// <summary>
        /// 获取作业记录
        /// </summary>
        /// <returns>The job record.</returns>
        /// <param name="jobConfig">Job config.</param>
        /// <param name="cluster">Cluster.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        JobBase GetJobRecord(JobConfig jobConfig, Cluster cluster, CancellationToken cancellationToken);

        /// <summary>
        /// 获取作业记录
        /// </summary>
        /// <returns>The job record.</returns>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        /// <param name="cluster">Cluster.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        JobBase GetJobRecord(string jobName, string jobId, Cluster cluster, CancellationToken cancellationToken);

        /// <summary>
        /// 取消作业记录
        /// </summary>
        /// <returns>The job record.</returns>
        bool CancelJobRecord(JobBase job);
    }
}
