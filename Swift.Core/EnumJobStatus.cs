using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Swift.Core
{
    /// <summary>
    /// 作业记录状态
    /// </summary>
    public enum EnumJobRecordStatus
    {
        /// <summary>
        /// 待处理
        /// </summary>
        Pending = 0,

        /// <summary>
        /// 正在制定计划
        /// </summary>
        PlanMaking = 1,

        /// <summary>
        /// 已制定计划
        /// </summary>
        PlanMaked = 2,

        /// <summary>
        /// 任务正在执行
        /// </summary>
        TaskExecuting = 3,

        /// <summary>
        /// 任务处理完毕
        /// </summary>
        TaskCompleted = 4,

        /// <summary>
        /// 任务同步完毕
        /// </summary>
        TaskSynced = 5,

        /// <summary>
        /// 任务合并完毕
        /// </summary>
        TaskMerged = 6,

        /// <summary>
        /// 制定计划失败
        /// </summary>
        PlanFailed = -1,

        /// <summary>
        /// 任务执行失败
        /// </summary>
        TaskExecutingFailed = -2,

        /// <summary>
        /// 任务合并失败
        /// </summary>
        TaskMergeFailed = -3,
    }
}
