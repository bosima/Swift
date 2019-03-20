using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Swift.Core
{
    /// <summary>
    /// 任务状态
    /// </summary>
    public enum EnumTaskStatus
    {
        /// <summary>
        /// 未处理
        /// </summary>
        Pending = 0,
        /// <summary>
        /// 处理中
        /// </summary>
        Executing = 1,
        /// <summary>
        /// 处理完成
        /// </summary>
        Completed = 2,
        /// <summary>
        /// 已同步任务结果
        /// </summary>
        Synced = 3,
        /// <summary>
        /// 正在取消
        /// </summary>
        Canceling = 4,
        /// <summary>
        /// 已取消
        /// </summary>
        Canceled = 5,
        /// <summary>
        /// 失败
        /// </summary>
        Failed = -1,
        /// <summary>
        /// 取消失败
        /// </summary>
        CancelFailed = -2,
        /// <summary>
        /// 同步任务结果失败
        /// </summary>
        SyncFailed = -3,
    }
}
