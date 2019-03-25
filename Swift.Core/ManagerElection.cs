using System;
using System.Text;
using System.Threading;
using Swift.Core.Consul;
using Swift.Core.Log;

namespace Swift.Core
{
    /// <summary>
    /// 当前节点参与Manager选举的结果事件
    /// </summary>
    public delegate void ManagerElectCompletedEvent(bool result, string currentManagerId);

    /// <summary>
    /// 管理员选举
    /// </summary>
    public class ManagerElection
    {
        private readonly string _clusterName = string.Empty;
        private readonly string _memberId = string.Empty;
        private string _session = string.Empty;
        private string _managerId = string.Empty;
        private readonly TimeSpan _defaultWatchInterval = new TimeSpan(0, 0, 60);
        private readonly TimeSpan _firstWatchInterval = new TimeSpan(0, 0, 3);
        private readonly TimeSpan _confirmWatchInterval = new TimeSpan(0, 0, 10);
        private const int _defaultOfflineConfirmAmount = 3;

        public event ManagerElectCompletedEvent ManagerElectCompletedEventHandler;

        public ManagerElection(string clusterName, string memberId)
        {
            _clusterName = clusterName;
            _memberId = memberId;
        }

        /// <summary>
        /// 获取管理员在Consul KV的键名
        /// </summary>
        /// <returns>The manager key.</returns>
        private string GetManagerKey()
        {
            return string.Format("Swift/{0}/Manager", _clusterName);
        }

        /// <summary>
        /// 监控Manager选举
        /// </summary>
        public void Watch(CancellationToken cancellationToken = default(CancellationToken))
        {
            var key = GetManagerKey();

            LogWriter.Write("start first manager election");

            // 上来就先选举一次，看看结果，以启动Manager或Worker剧本
            var electResult = Elect(out ulong modifyIndex, cancellationToken);
            ulong waitIndex = modifyIndex++;
            ManagerElectCompletedEventHandler?.Invoke(electResult, _managerId);

            var waitTime = _defaultWatchInterval;
            var offlineConfirmAmount = _defaultOfflineConfirmAmount;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    LogWriter.Write("start manager watch query", LogLevel.Debug);

                    // 阻塞查询
                    var kv = ConsulKV.BlockGet(key, waitTime, waitIndex, cancellationToken);

                    // 可能Key被删除了
                    if (kv == null)
                    {
                        LogWriter.Write("manager is not exists, start election right away");
                        electResult = Elect(out modifyIndex, cancellationToken);
                        waitIndex = modifyIndex++;
                        LogWriter.Write(string.Format("elect result: {0}, current manager: {1}", electResult, _managerId));
                        ManagerElectCompletedEventHandler?.Invoke(electResult, _managerId);
                    }
                    else
                    {
                        waitIndex = kv.ModifyIndex++;

                        // 如果Session为空，则说明Manager下线了
                        // 但是这时候可能Manager出现了闪断，为了减少Manager重新选举导致的处理工作，节点应该再确认两次，
                        // 如果在确认过程中Manager又上线了，则应该优先再次当选为Manager，减少不必要的麻烦
                        if (string.IsNullOrWhiteSpace(kv.Session))
                        {
                            LogWriter.Write(string.Format("manager session is empty, last manager is {0}", Encoding.UTF8.GetString(kv.Value)));

                            // 如果上一个Manager是当前成员，则马上重试
                            if (Encoding.UTF8.GetString(kv.Value) == _memberId)
                            {
                                LogWriter.Write("last manager is me, start election right away");
                                electResult = Elect(out modifyIndex, cancellationToken);
                                waitIndex = modifyIndex++;
                                LogWriter.Write(string.Format("elect result: {0}, current manager: {1}", electResult, _managerId));
                                ManagerElectCompletedEventHandler?.Invoke(electResult, _managerId);
                            }
                            else
                            {
                                if (offlineConfirmAmount == 0)
                                {
                                    LogWriter.Write("last manager not wake for a long time, start election right away", LogLevel.Info);
                                    electResult = Elect(out modifyIndex, cancellationToken);
                                    waitIndex = modifyIndex++;
                                    LogWriter.Write(string.Format("elect result: {0}, current manager: {1}", electResult, _managerId));
                                    ManagerElectCompletedEventHandler?.Invoke(electResult, _managerId);
                                }
                                else
                                {
                                    LogWriter.Write("wait last manager ...", LogLevel.Info);
                                    waitTime = _confirmWatchInterval;
                                    offlineConfirmAmount--;
                                    continue;
                                }
                            }
                        }
                    }

                    offlineConfirmAmount = _defaultOfflineConfirmAmount;
                    waitTime = _defaultWatchInterval;
                }
                catch (Exception ex)
                {
                    LogWriter.Write("Manager选举监控异常", ex);
                    Thread.Sleep(3000);
                }
            }
            while (true);
        }

        /// <summary>
        /// 重启集群选举
        /// </summary>
        public void Reset()
        {
            var key = GetManagerKey();
            ConsulKV.Delete(key);
        }

        /// <summary>
        /// 当前节点参选Manager
        /// </summary>
        /// <returns>The elect.</returns>
        private bool Elect(out ulong modifyIndex, CancellationToken cancellationToken = default(CancellationToken))
        {
            modifyIndex = 0;
            var electResult = false;

            // 创建一个关联到当前节点的Session
            if (!string.IsNullOrWhiteSpace(_session))
            {
                ConsulKV.RemoveSession(_session);
            }
            _session = ConsulKV.CreateSession(string.Format("CHECK:Swift-{0}-Member-{1}", _clusterName, _memberId), 0);

            // 获取选举要锁定的Consul KV对象
            var key = GetManagerKey();
            var kv = ConsulKV.Get(key, cancellationToken);
            if (kv == null)
            {
                kv = ConsulKV.Create(key);
            }

            if (string.IsNullOrWhiteSpace(kv.Session))
            {
                LogWriter.Write("Elect->Session为空，开始获取锁", LogLevel.Debug);

                // 绑定当前节点的Session去选举
                kv.Session = _session;
                kv.Value = Encoding.UTF8.GetBytes(_memberId);
                electResult = ConsulKV.Acquire(kv, cancellationToken);
            }

            // 无论参选成功与否，获取当前的Manager
            var managerKV = ConsulKV.Get(key, cancellationToken);
            if (managerKV != null)
            {
                modifyIndex = managerKV.ModifyIndex;
                _managerId = Encoding.UTF8.GetString(managerKV.Value);
            }

            LogWriter.Write("Elect->Current Manager: " + _managerId, LogLevel.Debug);

            return electResult;
        }
    }
}
