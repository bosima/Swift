using System;
using System.Threading;

namespace Swift.Core
{
    public interface IConfigCenter
    {
        /// <summary>
        /// 尝试更新作业实例状态
        /// </summary>
        /// <returns>The job status.</returns>
        /// <param name="job">Job.</param>
        /// <param name="status">Status.</param>
        /// <param name="errCode">错误代码：0无错误 1不能设置为这个状态 2无需重复设置状态 3作业不存在</param>
        /// <param name="latestJob">更新后最新的作业信息</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        bool TryUpdateJobStatus(JobBase job, EnumJobRecordStatus status, out int errCode, out JobBase latestJob, CancellationToken cancellationToken);

        /// <summary>
        /// 尝试更新任务状态
        /// </summary>
        /// <returns><c>true</c>, if update task status was tryed, <c>false</c> otherwise.</returns>
        /// <param name="task">Task.</param>
        /// <param name="status">Status.</param>
        /// <param name="errCode">错误代码：0无错误 1不能设置为这个状态 2无需重复设置状态 3作业或任务不存在</param>
        /// <param name="latestJob">Latest job.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        bool TryUpdateTaskStatus(JobTask task, EnumTaskStatus status, out int errCode, out JobBase latestJob, CancellationToken cancellationToken);

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
