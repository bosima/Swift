using System;
namespace Swift.Core.Election
{
    /// <summary>
    /// Manager选举状态
    /// </summary>
    public class ManagerElectionState
    {
        /// <summary>
        /// 选举状态数据
        /// </summary>
        public object Data { get; set; }

        /// <summary>
        /// Manager是否在线
        /// </summary>
        public bool IsManagerOnline { get; set; }

        /// <summary>
        /// 当前ManagerId
        /// </summary>
        public string CurrentManagerId { get; set; }
    }
}
