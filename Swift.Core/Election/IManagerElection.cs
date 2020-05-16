using System;
using System.Threading;

namespace Swift.Core.Election
{
    /// <summary>
    /// 管理员选举操作接口
    /// </summary>
    public interface IManagerElection
    {
        /// <summary>
        /// 执行选举操作
        /// </summary>
        /// <param name="cancellationToken"></param>
        ManagerElectionResult Elect(CancellationToken cancellationToken);

        /// <summary>
        /// 重新选举
        /// </summary>
        /// <param name="cancellationToken"></param>
        bool Reset(CancellationToken cancellationToken);

        /// <summary>
        /// 观察选举状态
        /// </summary>
        /// <param name="state">当前状态</param>
        /// <param name="processLatestState">处理最新状态的方法</param>
        /// <param name="cancellationToken"></param>
        void WatchState(ManagerElectionState state, Action<ManagerElectionState> processLatestState, CancellationToken cancellationToken);
    }
}
