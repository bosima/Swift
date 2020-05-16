using System;
using System.Threading;

namespace Swift.Core.Election
{
    public class ManagerElectionFactory
    {
        private readonly string _clusterName;
        private readonly string _currentMemberId;

        public ManagerElectionFactory(string clusterName, string currentMemberId)
        {
            _clusterName = clusterName;
            _currentMemberId = currentMemberId;
        }

        public IManagerElection Create(ManagerElectionOptions options)
        {
            if (options == null || string.IsNullOrWhiteSpace(options.ManagerElectionClass))
            {
                return new ConsulManagerElection(_clusterName, _currentMemberId);
            }

            throw new NotImplementedException("未实现自定义Manager选举类创建");
        }
    }
}
