using System;
using System.Threading;
using Swift.Core.Log;

namespace Swift.Core.Election
{
    public class ManagerElectionManager
    {
        private readonly IManagerElection _election;
        private readonly string _currentMemberId;
        private const int _defaultOfflineConfirmAmount = 3;

        public event ManagerElectCompletedEvent ManagerElectCompletedEventHandler;

        public ManagerElectionManager(string clusterName, string currentMemberId, ManagerElectionOptions options)
        {
            _currentMemberId = currentMemberId;
            var factory = new ManagerElectionFactory(clusterName, currentMemberId);
            _election = factory.Create(options);
        }

        public void Watch(CancellationToken cancellationToken = default)
        {
            // 上来就先选举一次，以获取要监控的状态
            var electState = Elect(cancellationToken);
            var offlineConfirmAmount = _defaultOfflineConfirmAmount;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    LogWriter.Write("manager state watch begin ...", LogLevel.Debug);

                    _election.WatchState(electState, newState =>
                    {
                        // 为空代表全局选举状态不存在或者被移除，则马上启动选举
                        if (newState == null)
                        {
                            LogWriter.Write("manager is not exists, start election right away");
                            electState = Elect(cancellationToken);
                            return;
                        }

                        // Manager下线
                        if (!newState.IsManagerOnline)
                        {
                            LogWriter.Write(string.Format("manager session is empty, last manager is {0}", electState.CurrentManagerId));

                            // 下线的Manager有优先选举权
                            if (newState.CurrentManagerId == _currentMemberId)
                            {
                                LogWriter.Write("last manager is me, start election right away");
                                electState = Elect(cancellationToken);
                                return;
                            }

                            // 其它节点需要确认Manager真的下线了才能发起选举
                            if (offlineConfirmAmount == 0)
                            {
                                LogWriter.Write("last manager not restore in a long time, start election right away", LogLevel.Info);
                                electState = Elect(cancellationToken);

                                // 有选举出新的Manager才需要重新确认
                                if (electState != null && electState.IsManagerOnline)
                                {
                                    offlineConfirmAmount = _defaultOfflineConfirmAmount;
                                }
                                return;
                            }

                            LogWriter.Write("wait last manager restore ...", LogLevel.Info);
                            offlineConfirmAmount--;

                            return;
                        }

                        offlineConfirmAmount = _defaultOfflineConfirmAmount;

                    }, cancellationToken);
                }
                catch (Exception ex)
                {
                    LogWriter.Write("Manager选举监控异常", ex);
                    Thread.Sleep(3000);
                }
            }
            while (true);
        }

        public void Reset(CancellationToken cancellationToken = default)
        {
            _election.Reset(cancellationToken);
        }

        private ManagerElectionState Elect(CancellationToken cancellationToken)
        {
            var electResult = _election.Elect(cancellationToken);
            var electState = electResult.State;
            LogWriter.Write(string.Format("elect result: {0}, current manager: {1}", electResult.IsSuccess, electState.CurrentManagerId));
            ManagerElectCompletedEventHandler?.Invoke(electResult);
            return electState;
        }
    }
}
