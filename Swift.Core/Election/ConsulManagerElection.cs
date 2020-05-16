using System;
using System.Text;
using System.Threading;
using Consul;
using Swift.Core.Consul;
using Swift.Core.Log;
using LogLevel = Swift.Core.Log.LogLevel;

namespace Swift.Core.Election
{
    public class ConsulManagerElection : IManagerElection
    {
        private string _electionKey = string.Empty;
        private string _sessionId = string.Empty;
        private string _sessionCheckId = string.Empty;
        private string _currentMemberId = string.Empty;
        private readonly TimeSpan _defaultWatchInterval = new TimeSpan(0, 0, 60);
        private readonly TimeSpan _confirmWatchInterval = new TimeSpan(0, 0, 10);

        public ConsulManagerElection(string clusterName, string currentMemberId)
        {
            _electionKey = string.Format("Swift/{0}/Manager", clusterName);
            _currentMemberId = currentMemberId;
            _sessionCheckId = string.Format("CHECK:Swift-{0}-Member-{1}", clusterName, currentMemberId);
        }

        public ManagerElectionResult Elect(CancellationToken cancellationToken = default)
        {
            bool lockResult = false;
            string currentManagerId = string.Empty;
            bool isCurrentManagerOnline = false;

            // 创建一个关联到当前节点的Session
            if (!string.IsNullOrWhiteSpace(_sessionId))
            {
                ConsulKV.RemoveSession(_sessionId);
            }
            _sessionId = ConsulKV.CreateSession(_sessionCheckId, 0);

            // 获取选举要锁定的Consul KV对象
            var kv = ConsulKV.Get(_electionKey, cancellationToken);
            if (kv == null)
            {
                kv = ConsulKV.Create(_electionKey);
            }

            if (string.IsNullOrWhiteSpace(kv.Session))
            {
                LogWriter.Write("Elect->Session为空，开始尝试锁定", LogLevel.Debug);
                kv.Session = _sessionId;
                kv.Value = Encoding.UTF8.GetBytes(_currentMemberId);
                lockResult = ConsulKV.Acquire(kv, cancellationToken);
                LogWriter.Write($"锁定结果:{lockResult}", LogLevel.Debug);
            }

            // 无论参选成功与否，获取当前的Manager
            var managerKV = ConsulKV.Get(_electionKey, cancellationToken);
            if (managerKV != null)
            {
                currentManagerId = Encoding.UTF8.GetString(managerKV.Value);
                isCurrentManagerOnline = !string.IsNullOrWhiteSpace(managerKV.Session);
            }

            LogWriter.Write($"Elect->当前Manager:{currentManagerId}", LogLevel.Debug);

            return new ManagerElectionResult()
            {
                State = new ManagerElectionState()
                {
                    Data = managerKV,
                    CurrentManagerId = currentManagerId,
                    IsManagerOnline = isCurrentManagerOnline,
                },
                IsSuccess = lockResult,
            };
        }

        public bool Reset(CancellationToken cancellationToken = default)
        {
            return ConsulKV.Delete(_electionKey, cancellationToken);
        }

        public void WatchState(ManagerElectionState state, Action<ManagerElectionState> processLatestState, CancellationToken cancellationToken = default)
        {
            KVPair consulKv = (KVPair)state.Data;
            ulong waitIndex = consulKv == null ? 0 : consulKv.ModifyIndex++;
            TimeSpan waitTime = state.IsManagerOnline ? _defaultWatchInterval : _confirmWatchInterval;
            var newConsulKv = ConsulKV.BlockGet(_electionKey, waitTime, waitIndex, cancellationToken);

            ManagerElectionState newState = null;
            if (newConsulKv != null)
            {
                newState = new ManagerElectionState
                {
                    Data = newConsulKv,
                    CurrentManagerId = Encoding.UTF8.GetString(newConsulKv.Value),
                    IsManagerOnline = !string.IsNullOrWhiteSpace(newConsulKv.Session)
                };
            }
            processLatestState?.Invoke(newState);
        }
    }
}
