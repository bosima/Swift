using System;
namespace Swift.Core.Election
{
    /// <summary>
    /// Manager选举设置项
    /// </summary>
    public class ManagerElectionOptions
    {
        /// <summary>
        /// Mangger选举类，格式：含命名空间的完整类名,所在程序集
        /// </summary>
        public string ManagerElectionClass { get; set; }
    }
}
