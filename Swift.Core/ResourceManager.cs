using System;
namespace Swift.Core
{
    /// <summary>
    /// 集群资源管理
    /// </summary>
    public class ResourceManager
    {
        // 每个节点上传自己的总可执行任务数到配置中心
        // Manager从配置中心获取每个节点的总可执行任务数和排队的任务数，从节点获取其正在执行的任务数
        // Manager分配任务时根据这个空闲任务数进行分配

        /// <summary>
        /// 获取当前节点可执行任务数限制
        /// </summary>
        /// <returns>The current execute amount limit.</returns>
        public static int GetCurrentExecuteAmountLimit()
        {
            return Environment.ProcessorCount;
        }
    }
}
