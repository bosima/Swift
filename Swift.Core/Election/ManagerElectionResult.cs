using System;
namespace Swift.Core.Election
{
    public class ManagerElectionResult
    {
        /// <summary>
        /// 当前节点是否选举成功
        /// </summary>
        public bool IsSuccess { get; set; }

        /// <summary>
        /// 选举状态
        /// </summary>
        public ManagerElectionState State { get; set; }
    }
}
