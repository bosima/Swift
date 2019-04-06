using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Consul;
using Newtonsoft.Json;
using Swift.Core.Consul;
using Swift.Core.ExtensionException;
using Swift.Core.Log;

namespace Swift.Core
{
    /// <summary>
    /// 作业配置加入事件
    /// </summary>
    /// <param name="jobConfig"></param>
    public delegate void JobConfigJoinEvent(JobConfig jobConfig, CancellationToken cancellationToken);

    /// <summary>
    /// 作业配置更新事件
    /// </summary>
    /// <param name="jobConfig"></param>
    public delegate void JobConfigUpdateEvent(JobConfig jobConfig, CancellationToken cancellationToken);

    /// <summary>
    /// 作业配置移除事件
    /// </summary>
    /// <param name="jobConfig"></param>
    public delegate void JobConfigRemoveEvent(JobConfig jobConfig, CancellationToken cancellationToken);

    /// <summary>
    /// 集群
    /// </summary>
    public class Cluster
    {
        private readonly string name;
        private string localIP;
        private List<Member> members;
        private readonly object membersLocker = new object();
        private List<JobBase> activedJobs;
        private readonly object activedJobsLocker = new object();
        private List<JobConfig> jobConfigs;
        private readonly object jobConfigsLocker = new object();
        private Member currentMember;
        private readonly object refreshCenterConfigLocker = new object();
        private readonly IConfigCenter configCenter;

        /// <summary>
        /// 作业配置加入事件。
        /// </summary>
        public event JobConfigJoinEvent OnJobConfigJoinEventHandler;

        /// <summary>
        /// 作业配置更新事件。
        /// </summary>
        public event JobConfigUpdateEvent OnJobConfigUpdateEventHandler;

        /// <summary>
        /// 作业配置移除事件。
        /// </summary>
        public event JobConfigRemoveEvent OnJobConfigRemoveEventHandler;

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="name"></param>
        public Cluster(string name, string bindingIP)
        {
            configCenter = new ConsulConfigCenter();
            this.name = name;
            this.localIP = bindingIP;
            this.members = new List<Member>();
            this.activedJobs = new List<JobBase>();
            this.jobConfigs = new List<JobConfig>();
        }

        /// <summary>
        /// 获取集群的配置中心
        /// </summary>
        /// <value>The config center.</value>
        public IConfigCenter ConfigCenter
        {
            get
            {
                return configCenter;
            }
        }

        /// <summary>
        /// 获取集群名称
        /// </summary>
        public string Name
        {
            get
            {
                return this.name;
            }
        }

        /// <summary>
        /// 获取本机IP
        /// </summary>
        public string LocalIP
        {
            get
            {
                return localIP;
            }
        }

        /// <summary>
        /// 当前成员
        /// </summary>
        public Member CurrentMember
        {
            get
            {
                return currentMember;
            }
        }

        /// <summary>
        /// 获取集群成员列表
        /// </summary>
        public List<Member> Members
        {
            get
            {
                return members;
            }
        }

        /// <summary>
        /// 获取作业配置列表
        /// </summary>
        public List<JobConfig> JobConfigs
        {
            get
            {
                return jobConfigs;
            }
        }

        /// <summary>
        /// 加载集群配置
        /// </summary>
        public void Init()
        {
            if (string.IsNullOrWhiteSpace(this.localIP))
            {
                this.localIP = GetLocalIP();
            }

            RegisterToConsul();
        }

        #region 成员注册
        /// <summary>
        /// 注册为Member
        /// </summary>
        /// <returns></returns>
        public Member RegisterMember(string memberId)
        {
            Member member = new Member()
            {
                Id = memberId,
                OnlineTime = DateTime.Now,
                Status = 1,
            };

            return RegisterMember(member);
        }

        /// <summary>
        /// 注册成员
        /// </summary>
        /// <param name="member"></param>
        private Member RegisterMember(Member member)
        {
            int tryTimes = 3;
            while (tryTimes > 0)
            {
                try
                {
                    List<Member> latestMembers;
                    while (!TryRegisterMemberToConfigCenter(member, out latestMembers))
                    {
                        Thread.Sleep(1000);
                    }

                    members = latestMembers;
                    break;
                }
                catch (Exception ex)
                {
                    tryTimes--;
                    LogWriter.Write(string.Format("注册成员出现异常，还会重试{0}次：{1}", tryTimes, ex.Message));
                    Thread.Sleep(2000);
                }
            }

            currentMember = members.FirstOrDefault(d => d.Id == member.Id);
            return currentMember;
        }

        /// <summary>
        /// 尝试注册成员到Consul
        /// </summary>
        /// <returns></returns>
        private bool TryRegisterMemberToConfigCenter(Member member, out List<Member> latestMemberList)
        {
            latestMemberList = null;

            List<Member> memberList = GetMembersFromConfigCenter();

            Member historyMember = memberList.FirstOrDefault(d => d.Id == member.Id);
            if (historyMember != null)
            {
                historyMember.Status = 1;
                historyMember.OnlineTime = DateTime.Now;
                historyMember.Cluster = this;
            }
            else
            {
                member.FirstRegisterTime = DateTime.Now;
                member.Cluster = this;
                memberList.Add(member);
            }

            // 更新到Consul中
            var memberKey = string.Format("Swift/{0}/Members", Name);
            KVPair memberKV = ConsulKV.Get(memberKey);
            if (memberKV == null)
            {
                memberKV = ConsulKV.Create(memberKey);
            }

            memberKV.Value = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(memberList));
            var result = ConsulKV.CAS(memberKV);
            if (result)
            {
                latestMemberList = memberList;
            }

            return result;
        }

        /// <summary>
        /// 注册Swift成员到Consul
        /// </summary>
        private void RegisterToConsul()
        {
            var serviceId = string.Format("Swift-{0}-Member-{1}", Name, localIP);
            var serviceName = string.Format("Swift-{0}-Member", Name);
            ConsulService.RegisterService(serviceId, serviceName, 20);
        }
        #endregion

        #region 监控成员健康状况并更新到配置中心

        /// <summary>
        /// 检查成员状态定时器
        /// </summary>
        private Timer checkMembersTimer;

        /// <summary>
        /// 指示检查成员状态定时器是否可用
        /// </summary>
        private CancellationTokenSource _checkMembersTimerCts;

        /// <summary>
        /// 监控成员健康状况
        /// </summary>
        public void MonitorMembersHealth()
        {
            LogWriter.Write("the timer of check member health is starting ...");

            if (_checkMembersTimerCts == null
             || _checkMembersTimerCts.IsCancellationRequested)
            {
                _checkMembersTimerCts = new CancellationTokenSource();
            }

            StartCheckMembersTimer(_checkMembersTimerCts.Token);
        }

        /// <summary>
        /// 停止监控成员健康状态
        /// </summary>
        public void StopMonitorMemberHealth()
        {
            if (!_checkMembersTimerCts.IsCancellationRequested)
            {
                _checkMembersTimerCts.Cancel();
            }

            StopCheckMembersTimer();

            LogWriter.Write("the timer of check member health has stopped");
        }

        /// <summary>
        /// 定时检查成员状态回调
        /// </summary>
        /// <param name="state">State.</param>
        private void TimedCheckMembersHealthCallback(object state, CancellationToken cancellationToken = default)
        {
            StopCheckMembersTimer();

            CheckMembersHealth(cancellationToken);

            StartCheckMembersTimer(cancellationToken);
        }

        /// <summary>
        /// 检查成员健康状况
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        public void CheckMembersHealth(CancellationToken cancellationToken = default)
        {
            try
            {
                LogWriter.Write("check member health beginning ...");

                // 检查成员健康状态，如果成员健康状态变化则尝试更新到Consul KV，直到成员状态和Consul KV中一致。
                while (!CheckMemberHealth(cancellationToken))
                {
                    LogWriter.Write("更新成员健康状态到Consul失败，Consul数据已改变，稍后重试");
                    Thread.Sleep(300);
                }

                LogWriter.Write("check member health completed.");
            }
            catch (Exception ex)
            {
                LogWriter.Write("成员健康检查异常。", ex);
            }
        }

        /// <summary>
        /// 检查成员健康状态，如果改变则尝试更新到Consul。
        /// 通过Consul健康检查获取健康状态，并且仅更新到Consul KV，不更新本地内存缓存节点健康状态。
        /// 本地内存缓存节点健康状态需要另一个定时器来更新。
        /// </summary>
        /// <returns><c>true</c>, if member health was checked, <c>false</c> otherwise.</returns>
        private bool CheckMemberHealth(CancellationToken cancellationToken = default)
        {
            object orignalData = null;
            bool hasChange = false;
            List<MemberWrapper> configMemberList = null;

            do
            {
                configMemberList = GetMemberListFromConfigCenter(out orignalData, cancellationToken);

                Dictionary<string, bool> healths = GetMembersHealth(cancellationToken);

                hasChange = false;
                hasChange = MakeMembersStatus(configMemberList, healths, cancellationToken);

            } while (!UpdateMemberListToConfigCenter(hasChange, orignalData, configMemberList, cancellationToken));

            return true;
        }

        /// <summary>
        /// 更新成员列表到配置中心
        /// </summary>
        /// <returns><c>true</c>, if member list to config center was updated, <c>false</c> otherwise.</returns>
        /// <param name="hasChange">If set to <c>true</c> has change.</param>
        /// <param name="orignalData">Orignal data.</param>
        /// <param name="configMemberList">Config member list.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private bool UpdateMemberListToConfigCenter(bool hasChange, object orignalData, List<MemberWrapper> configMemberList, CancellationToken cancellationToken = default)
        {
            if (!hasChange)
            {
                LogWriter.Write("成员没有变化");
                return true;
            }

            LogWriter.Write("成员健康状态出现变化，将要更新到Consul...");
            return UpdateMemberListToConfigCenter(orignalData, configMemberList, cancellationToken);
        }

        /// <summary>
        /// Updates the member list to config center.
        /// </summary>
        /// <returns><c>true</c>, if member list to config center was updated, <c>false</c> otherwise.</returns>
        /// <param name="orignalData">Orignal data.</param>
        /// <param name="configMemberList">Config member list.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private bool UpdateMemberListToConfigCenter(object orignalData, List<MemberWrapper> configMemberList, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            KVPair memberKV = (KVPair)orignalData;
            memberKV.Value = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(configMemberList));
            return ConsulKV.CAS(memberKV, cancellationToken);
        }

        /// <summary>
        /// Gets the member list from config center.
        /// </summary>
        /// <returns>The member list from config center.</returns>
        /// <param name="orignalData">Orignal data.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private List<MemberWrapper> GetMemberListFromConfigCenter(out object orignalData, CancellationToken cancellationToken)
        {
            orignalData = null;
            var configMemberList = new List<MemberWrapper>();

            var memberKey = string.Format("Swift/{0}/Members", Name);
            var memberKV = ConsulKV.Get(memberKey, cancellationToken);
            if (memberKV == null)
            {
                memberKV = ConsulKV.Create(memberKey);
            }

            if (memberKV.Value != null)
            {
                configMemberList = JsonConvert.DeserializeObject<List<MemberWrapper>>(Encoding.UTF8.GetString(memberKV.Value));
            }

            orignalData = memberKV;

            return configMemberList;
        }

        /// <summary>
        /// Gets the members health.
        /// </summary>
        /// <returns>The members health.</returns>
        /// <param name="cancellationToken">Cancellation token.</param>
        private Dictionary<string, bool> GetMembersHealth(CancellationToken cancellationToken)
        {
            var serviceName = string.Format("Swift-{0}-Member", Name);
            var healths = ConsulService.GetHealths(serviceName, cancellationToken);
            LogWriter.Write("Swift Node Health Info: " + JsonConvert.SerializeObject(healths), Log.LogLevel.Trace);
            return healths;
        }

        /// <summary>
        /// Makes the members status.
        /// </summary>
        /// <returns><c>true</c>, if members status was made, <c>false</c> otherwise.</returns>
        /// <param name="configMemberList">Config member list.</param>
        /// <param name="healths">Healths.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private bool MakeMembersStatus(List<MemberWrapper> configMemberList, Dictionary<string, bool> healths, CancellationToken cancellationToken = default)
        {
            bool isNeedUpdate = false;

            List<MemberWrapper> needRemoveList = new List<MemberWrapper>();
            foreach (var configMember in configMemberList)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var oldStatus = configMember.Status;

                var serviceId = string.Format("Swift-{0}-Member-{1}", Name, configMember.Id);
                healths.TryGetValue(serviceId, out bool isHealth);
                configMember.Status = (isHealth ? 1 : 0);
                LogWriter.Write(string.Format("成员新旧健康状态:{0},{1}->{2}", serviceId, oldStatus, configMember.Status), Log.LogLevel.Debug);

                if (configMember.Status != oldStatus)
                {
                    var isChange = MakeMemberStatus(configMember, oldStatus, out bool needRemove);

                    if (isChange && !isNeedUpdate)
                    {
                        isNeedUpdate = true;
                    }

                    if (needRemove)
                    {
                        needRemoveList.Add(configMember);
                    }
                }
            }

            // 从集合中移除过期的成员
            if (needRemoveList.Count > 0)
            {
                foreach (var removeConfig in needRemoveList)
                {
                    configMemberList.Remove(removeConfig);
                }
            }

            return isNeedUpdate;
        }

        /// <summary>
        /// 生成成员状态
        /// </summary>
        /// <returns><c>true</c>, if member status change, <c>false</c> otherwise.</returns>
        /// <param name="configMember">Config member.</param>
        /// <param name="oldStatus">Old status.</param>
        /// <param name="needRemove">If set to <c>true</c> need remove.</param>
        private static bool MakeMemberStatus(MemberWrapper configMember, int oldStatus, out bool needRemove)
        {
            needRemove = false;
            bool isChange = false;

            if (configMember.Status != oldStatus)
            {
                if (configMember.Status == 1)
                {
                    isChange = true;
                    configMember.OnlineTime = DateTime.Now;
                }

                if (configMember.Status == 0)
                {
                    isChange = true;
                    configMember.OfflineTime = DateTime.Now;
                }
            }
            else
            {
                if (configMember.Status == 0)
                {
                    // 离线超过30分钟了，移除成员
                    if (DateTime.Now.Subtract(configMember.OfflineTime.Value).TotalMinutes > 30)
                    {
                        isChange = true;
                        needRemove = true;
                    }
                }
            }

            return isChange;
        }

        /// <summary>
        /// 开启成员健康检查Timer
        /// </summary>
        private void StartCheckMembersTimer(CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            if (checkMembersTimer == null)
            {
                checkMembersTimer = new Timer(new TimerCallback(state => TimedCheckMembersHealthCallback(state, cancellationToken)), null, SwiftConfiguration.CheckMemberInterval, SwiftConfiguration.CheckMemberInterval);
            }
        }

        /// <summary>
        /// 关闭成员健康检查Timer
        /// </summary>
        private void StopCheckMembersTimer()
        {
            if (checkMembersTimer != null)
            {
                checkMembersTimer.Dispose();
                checkMembersTimer = null;
            }
        }
        #endregion

        #region 监控本地作业配置变化并更新到配置中心
        /// <summary>
        /// 本地磁盘作业配置刷新定时器
        /// </summary>
        private Timer refreshJobConfigFromDiskTimer;

        /// <summary>
        /// 指示本地磁盘作业配置刷新定时器的启用状态
        /// </summary>
        private CancellationTokenSource _refreshJobConfigFromDiskTimerCts;

        /// <summary>
        /// 监控本地磁盘作业配置变化
        /// </summary>
        public void MonitorJobConfigsFromDisk()
        {
            LogWriter.Write(string.Format("开启监控本地磁盘作业配置变化..."));

            if (_refreshJobConfigFromDiskTimerCts == null
            || _refreshJobConfigFromDiskTimerCts.IsCancellationRequested)
            {
                _refreshJobConfigFromDiskTimerCts = new CancellationTokenSource();
            }

            StartRefreshJobConfigFromDiskTimer(_refreshJobConfigFromDiskTimerCts.Token);
        }

        /// <summary>
        /// 停止监控本地磁盘作业配置变化
        /// </summary>
        public void StopMonitorJobConfigsFromDisk()
        {
            if (!_refreshJobConfigFromDiskTimerCts.IsCancellationRequested)
            {
                _refreshJobConfigFromDiskTimerCts.Cancel();
            }

            StopRefreshJobConfigFromDiskTimer();

            LogWriter.Write(string.Format("已停止监控本地磁盘作业配置变化"));
        }

        /// <summary>
        /// 刷新本地磁盘作业配置
        /// </summary>
        private void TimedRefreshJobConfigsFromDiskCallback(object state, CancellationToken cancellationToken = default)
        {
            StopRefreshJobConfigFromDiskTimer();

            try
            {
                var diskJobConfigs = LoadJobConfigsFromDisk(cancellationToken);
                var consulJobConfigs = GetJobConfigsFromConfigCenter();

                LogWriter.Write(string.Format("本地磁盘作业配置数量:{0}", diskJobConfigs.Count));
                LogWriter.Write(string.Format("配置中心作业配置数量:{0}", consulJobConfigs.Count));

                foreach (var diskJobConfig in diskJobConfigs)
                {
                    LogWriter.Write("开始检查作业配置:" + diskJobConfig.Name);
                    var setResult = AddOrUpdateJobConfigToConfigCenter(diskJobConfig);
                    LogWriter.Write("已处理作业配置:" + diskJobConfig.Name + "," + setResult);
                }

                List<JobConfig> removeJobConfigList = new List<JobConfig>();
                foreach (var consulJobConfig in consulJobConfigs)
                {
                    if (!diskJobConfigs.Any(d => d.Name == consulJobConfig.Name))
                    {
                        var configRemoveResult = RemoveJobConfigFromConfigCenter(consulJobConfig);
                        LogWriter.Write("移除配置中心作业配置:" + consulJobConfig.Name + "," + configRemoveResult);
                    }
                }
            }
            catch (Exception ex)
            {
                LogWriter.Write(string.Format("刷新本地磁盘作业配置到配置中心异常:{0}", ex.Message), ex);
            }

            StartRefreshJobConfigFromDiskTimer();
        }

        /// <summary>
        /// 从作业包更新作业配置文件到最新
        /// </summary>
        /// <param name="pkgPath">File path.</param>
        private void UpdateLocalJobConfig(string pkgPath, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int lastSeparatorIndex = pkgPath.LastIndexOf(Path.DirectorySeparatorChar);
            var pkgDirPath = pkgPath.Substring(0, lastSeparatorIndex);
            var pkgJsonName = pkgPath.Substring(lastSeparatorIndex + 1, pkgPath.LastIndexOf('.') - (lastSeparatorIndex + 1));
            var jsonPath = Path.Combine(pkgDirPath, pkgJsonName + ".json");

            EnsureJobPackageConfigFileExists(pkgPath, jsonPath, cancellationToken);

            var latestJobConfig = new JobConfig(jsonPath);

            UpdateLocalJobConfig(latestJobConfig, cancellationToken);
        }

        /// <summary>
        /// Ensures the job package config file exists.
        /// </summary>
        /// <param name="pkgPath">Package path.</param>
        /// <param name="jsonPath">Json path.</param>
        private void EnsureJobPackageConfigFileExists(string pkgPath, string jsonPath, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!File.Exists(jsonPath))
            {
                LogWriter.Write(string.Format("正在尝试解压作业包的配置到:{0}", jsonPath));

                using (var zip = ZipFile.Open(pkgPath, ZipArchiveMode.Read))
                {
                    zip.GetEntry("job.json").ExtractToFile(jsonPath, true);
                }
            }
        }

        /// <summary>
        /// 更新本地作业配置
        /// </summary>
        private void UpdateLocalJobConfig(JobConfig jobConfig, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            string jobConfigDirPath = Path.Combine(SwiftConfiguration.BaseDirectory, "Jobs", jobConfig.Name, "config");
            if (!Directory.Exists(jobConfigDirPath))
            {
                Directory.CreateDirectory(jobConfigDirPath);
            }

            string jobConfigPath = Path.Combine(jobConfigDirPath, "job.json");
            LogWriter.Write(string.Format("正在尝试更新作业包的配置:{0}", jobConfigPath));

            var configLockName = SwiftConfiguration.GetFileOperateLockName(jobConfigPath);
            lock (string.Intern(configLockName))
            {
                JobConfig newJobConfig = jobConfig;
                if (File.Exists(jobConfigPath))
                {
                    newJobConfig = new JobConfig(jobConfigPath)
                    {
                        FileName = jobConfig.FileName,
                        JobClassName = jobConfig.JobClassName,
                        RunTimePlan = jobConfig.RunTimePlan,
                        Version = jobConfig.Version,
                        MemberUnavailableThreshold = jobConfig.MemberUnavailableThreshold,
                        TaskExecuteTimeout = jobConfig.TaskExecuteTimeout,
                        JobSplitTimeout = jobConfig.JobSplitTimeout,
                        TaskResultCollectTimeout = jobConfig.TaskResultCollectTimeout,
                    };
                }

                File.WriteAllTextAsync(jobConfigPath, JsonConvert.SerializeObject(newJobConfig), cancellationToken).Wait();
            }

            LogWriter.Write(string.Format("作业包配置已更新完毕:{0}", jobConfigPath));
        }

        /// <summary>
        /// 从磁盘加载作业配置
        /// </summary>
        /// <returns></returns>
        private List<JobConfig> LoadJobConfigsFromDisk(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            List<JobConfig> latestJobConfigs = new List<JobConfig>();

            var jobRootPath = Path.Combine(SwiftConfiguration.BaseDirectory, "Jobs");
            if (!Directory.Exists(jobRootPath))
            {
                LogWriter.Write(string.Format("作业包目录为空，不能再轻松了。"));
                return latestJobConfigs;
            }

            // 查找作业包配置
            var jobDirPaths = Directory.GetDirectories(jobRootPath);
            foreach (var jobDirPath in jobDirPaths)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (jobDirPath.EndsWith(Path.DirectorySeparatorChar + "process", StringComparison.Ordinal))
                {
                    continue;
                }

                EnsureJobConfigUpToDate(jobDirPath, cancellationToken);
                JobConfig jobConfig = GetJobConfig(jobDirPath, cancellationToken);

                latestJobConfigs.Add(jobConfig);
            }

            return latestJobConfigs;
        }

        /// <summary>
        /// 获取作业配置
        /// </summary>
        /// <returns>The job config.</returns>
        /// <param name="jobDirPath">Job dir path.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private static JobConfig GetJobConfig(string jobDirPath, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var jobConfigFilePath = Path.Combine(jobDirPath, "config", "job.json");
            var configLockName = SwiftConfiguration.GetFileOperateLockName(jobConfigFilePath);
            JobConfig jobConfig;
            lock (string.Intern(configLockName))
            {
                jobConfig = new JobConfig(jobConfigFilePath);
            }

            return jobConfig;
        }

        /// <summary>
        /// 确保作业配置是最新的
        /// </summary>
        /// <param name="jobDirPath">Job dir path.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private void EnsureJobConfigUpToDate(string jobDirPath, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // 使用最新作业包配置
            var jobPkgDirPath = Path.Combine(jobDirPath, "packages");
            var latestJobPkgPath = Directory.GetFiles(jobPkgDirPath, "*.zip").OrderByDescending(d => d).FirstOrDefault();
            LogWriter.Write("发现最新作业包为：" + (string.IsNullOrWhiteSpace(latestJobPkgPath) ? jobPkgDirPath : latestJobPkgPath));

            if (latestJobPkgPath != null)
            {
                UpdateLocalJobConfig(latestJobPkgPath, cancellationToken);
            }
        }

        /// <summary>
        /// 添加或更新作业配置到配置中心
        /// </summary>
        /// <param name="jobConfig"></param>
        /// <returns></returns>
        private bool AddOrUpdateJobConfigToConfigCenter(JobConfig jobConfig, CancellationToken cancellationToken = default)
        {
            KVPair configKV = null;

            do
            {
                var jobConfigKey = string.Format("Swift/{0}/Jobs/{1}/Config", Name, jobConfig.Name);
                configKV = ConsulKV.Get(jobConfigKey, cancellationToken);
                if (configKV == null)
                {
                    configKV = ConsulKV.Create(jobConfigKey);
                }

                JobConfig oldJobConfig = null;
                if (configKV.Value != null)
                {
                    oldJobConfig = JsonConvert.DeserializeObject<JobConfig>(Encoding.UTF8.GetString(configKV.Value));
                }

                if (oldJobConfig != null)
                {
                    // 如果版本号没有改变，则作业配置没有变化，无需更新配置中心
                    if (oldJobConfig.Version == jobConfig.Version)
                    {
                        LogWriter.Write("作业配置版本号无变化，无需同步到配置中心。");
                        break;
                    }

                    jobConfig.LastRecordId = oldJobConfig.LastRecordId;
                    jobConfig.LastRecordCreateTime = oldJobConfig.LastRecordCreateTime;
                    jobConfig.ModifyIndex = oldJobConfig.ModifyIndex;
                }

                configKV.Value = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(jobConfig));

            } while (!ConsulKV.CAS(configKV, cancellationToken));

            return true;
        }

        /// <summary>
        /// 从配置中心移除作业配置，成功返回true，否则返回false
        /// </summary>
        /// <param name="jobConfig"></param>
        /// <returns></returns>
        private bool RemoveJobConfigFromConfigCenter(JobConfig jobConfig, CancellationToken cancellationToken = default)
        {
            var jobConfigKey = string.Format("Swift/{0}/Jobs/{1}", Name, jobConfig.Name);
            return ConsulKV.DeleteTree(jobConfigKey, cancellationToken);
        }

        /// <summary>
        /// 开启成员健康检查Timer
        /// </summary>
        private void StartRefreshJobConfigFromDiskTimer(CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            if (refreshJobConfigFromDiskTimer == null)
            {
                refreshJobConfigFromDiskTimer = new Timer(new TimerCallback(state => TimedRefreshJobConfigsFromDiskCallback(state, cancellationToken)), null, SwiftConfiguration.RefreshJobConfigsInterval, SwiftConfiguration.RefreshJobConfigsInterval);
            }
        }

        /// <summary>
        /// 关闭成员健康检查Timer
        /// </summary>
        private void StopRefreshJobConfigFromDiskTimer()
        {
            if (refreshJobConfigFromDiskTimer != null)
            {
                refreshJobConfigFromDiskTimer.Dispose();
                refreshJobConfigFromDiskTimer = null;
            }
        }
        #endregion

        #region 创建作业并更新到配置中心
        /// <summary>
        /// 作业创建定时器
        /// </summary>
        private Timer jobCreateTimer;

        /// <summary>
        /// The job create locker.
        /// </summary>
        private readonly object jobCreateLocker = new object();

        /// <summary>
        /// 指示作业创建定时器的启用状态
        /// </summary>
        private CancellationTokenSource _jobCreateTimerCts;

        /// <summary>
        /// 监控作业记录创建
        /// </summary>
        public void MonitorJobCreate()
        {
            LogWriter.Write(string.Format("timed create job is starting ..."));

            if (_jobCreateTimerCts == null || _jobCreateTimerCts.IsCancellationRequested)
            {
                _jobCreateTimerCts = new CancellationTokenSource();
            }

            StartCreateJobTimer(_jobCreateTimerCts.Token);
        }

        /// <summary>
        /// 停止监控作业记录创建
        /// </summary>
        public void StopMonitorJobCreate()
        {
            if (!_jobCreateTimerCts.IsCancellationRequested)
            {
                _jobCreateTimerCts.Cancel();
            }

            StopCreateJobTimer();
            LogWriter.Write(string.Format("timed create job has stopped"));
        }

        /// <summary>
        /// 定时创建作业
        /// </summary>
        /// <param name="state"></param>
        private void TimedCreateJobCallback(object state, CancellationToken cancellationToken = default)
        {
            StopCreateJobTimer();

            var latestJobConfigs = GetLatestJobConfigs();
            if (latestJobConfigs != null && latestJobConfigs.Length > 0)
            {
                LogWriter.Write(string.Format("check and create job beginning ..."));

                foreach (var jobConfig in latestJobConfigs)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (jobConfig.RunTimes == null || jobConfig.RunTimes.Length <= 0)
                    {
                        continue;
                    }

                    try
                    {
                        foreach (var timePlan in jobConfig.RunTimes)
                        {
                            if (timePlan.CheckIsTime(jobConfig.LastRecordCreateTime))
                            {
                                CheckAndCreateNewJobRecord(jobConfig, cancellationToken);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        LogWriter.Write("作业创建检查异常", ex);
                    }
                }

                LogWriter.Write(string.Format("check and create job end"));
            }

            StartCreateJobTimer(cancellationToken);
        }

        /// <summary>
        /// 获取最后一次记录
        /// </summary>
        /// <returns>The last job record.</returns>
        /// <param name="jobConfig">Job config.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private JobBase GetLastJobRecord(JobConfig jobConfig, CancellationToken cancellationToken = default)
        {
            var lastJobRecord = ConfigCenter.GetJobRecord(jobConfig, this, cancellationToken);
            return lastJobRecord;
        }

        /// <summary>
        /// Checks the and create new job record.
        /// </summary>
        /// <param name="jobName">Job name.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public bool CheckAndCreateNewJobRecord(string jobName, CancellationToken cancellationToken = default)
        {
            var jobConfig = GetJobConfig(jobName);
            return CheckAndCreateNewJobRecord(jobConfig, cancellationToken);
        }

        /// <summary>
        /// 检查及创建新的作业记录
        /// </summary>
        /// <param name="jobConfig">Job config.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private bool CheckAndCreateNewJobRecord(JobConfig jobConfig, CancellationToken cancellationToken = default)
        {
            lock (jobCreateLocker)
            {
                var lastJobRecord = GetLastJobRecord(jobConfig, cancellationToken);
                if (lastJobRecord != null
                && lastJobRecord.Status != EnumJobRecordStatus.TaskMerged
                && lastJobRecord.Status != EnumJobRecordStatus.Canceled
                && lastJobRecord.Status != EnumJobRecordStatus.PlanFailed
                && lastJobRecord.Status != EnumJobRecordStatus.TaskExecutingFailed
                && lastJobRecord.Status != EnumJobRecordStatus.TaskMergeFailed
                && lastJobRecord.Status != EnumJobRecordStatus.CancelFailed)
                {
                    LogWriter.Write(string.Format("上一次作业记录未完成:{0},{1},{2}", jobConfig.Name, jobConfig.LastRecordId, lastJobRecord.Status), Log.LogLevel.Warn);
                    return false;
                }

                var newJob = JobBase.CreateInstance(jobConfig, this);
                ConfigCenter.UpdateLastJobRecord(newJob, cancellationToken);

                return true;
            }
        }

        /// <summary>
        /// 开启作业创建Timer
        /// </summary>
        private void StartCreateJobTimer(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (jobCreateTimer == null)
            {
                jobCreateTimer = new Timer(new TimerCallback(state => TimedCreateJobCallback(state, cancellationToken)), null, SwiftConfiguration.JobSpaceCreateInterval, SwiftConfiguration.JobSpaceCreateInterval);
            }
        }

        /// <summary>
        /// 关闭作业创建Timer
        /// </summary>
        private void StopCreateJobTimer()
        {
            if (jobCreateTimer != null)
            {
                jobCreateTimer.Dispose();
                jobCreateTimer = null;
            }
        }
        #endregion

        #region 从配置中心获取最新集群成员
        /// <summary>
        /// 获取管理员在Consul KV的键名
        /// </summary>
        /// <returns>The manager key.</returns>
        private string GetManagerKey()
        {
            return string.Format("Swift/{0}/Manager", name);
        }

        /// <summary>
        /// 获取管理员
        /// </summary>
        /// <returns>The manager.</returns>
        public Member Manager
        {
            get
            {
                var key = GetManagerKey();
                var managerId = ConsulKV.GetValueString(key);
                return GetLatestMembers().FirstOrDefault(d => d.Id == managerId);
            }
        }

        /// <summary>
        /// 获取最近的工人数组
        /// </summary>
        /// <returns>The manager.</returns>
        public Member[] GetLatestWorkers(CancellationToken cancellationToken = default)
        {
            var key = GetManagerKey();
            var managerId = ConsulKV.GetValueString(key, cancellationToken);

            return GetLatestMembers().Where(d => d.Id != managerId).ToArray();
        }

        /// <summary>
        /// 获取最新的成员信息
        /// </summary>
        /// <returns>The current members.</returns>
        public Member[] GetLatestMembers(CancellationToken cancellationToken = default)
        {
            lock (membersLocker)
            {
                try
                {
                    members = GetMembersFromConfigCenter(cancellationToken);
                }
                catch (Exception ex)
                {
                    LogWriter.Write("从配置中心获取集群的所有成员异常", ex);
                }

                return members.ToArray();
            }
        }

        /// <summary>
        /// 从Consul获取集群成员
        /// </summary>
        private List<Member> GetMembersFromConfigCenter(CancellationToken cancellationToken = default)
        {
            var memberKey = string.Format("Swift/{0}/Members", Name);
            KVPair memberKV = ConsulKV.Get(memberKey, cancellationToken);
            if (memberKV == null)
            {
                memberKV = ConsulKV.Create(memberKey);
            }

            var configMemberList = new List<MemberWrapper>();
            if (memberKV.Value != null)
            {
                configMemberList = JsonConvert.DeserializeObject<List<MemberWrapper>>(Encoding.UTF8.GetString(memberKV.Value));
            }

            List<Member> memberList = new List<Member>();
            foreach (var configMember in configMemberList)
            {
                configMember.Cluster = this;
                memberList.Add(configMember);
            }

            return memberList;
        }
        #endregion

        #region 从配置中心更新作业配置到本地
        /// <summary>
        /// 移除作业
        /// </summary>
        /// <param name="jobConfig">Job config.</param>
        public void RemoveJobConfig(JobConfig jobConfig, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var jobRootPath = SwiftConfiguration.GetJobRootPath(jobConfig.Name);

            try
            {
                if (Directory.Exists(jobRootPath))
                {
                    Directory.Delete(jobRootPath, true);
                }
            }
            catch (Exception ex)
            {
                LogWriter.Write("移除作业异常:" + ex.Message, ex);
            }
        }

        /// <summary>
        /// 获取最新的作业配置数组
        /// </summary>
        /// <returns>The current job configs.</returns>
        public JobConfig[] GetLatestJobConfigs()
        {
            lock (jobConfigsLocker)
            {
                return jobConfigs.ToArray();
            }
        }

        /// <summary>
        /// 获取作业配置
        /// </summary>
        /// <returns>The last job record.</returns>
        /// <param name="jobName">Job name.</param>
        private JobConfig GetJobConfig(string jobName)
        {
            JobConfig jobConfig = null;
            var latestJobConfigs = GetLatestJobConfigs();
            if (latestJobConfigs != null)
            {
                jobConfig = latestJobConfigs.FirstOrDefault(d => d.Name == jobName);
            }

            return jobConfig;
        }

        /// <summary>
        /// 从Consul刷新作业配置定时器
        /// </summary>
        private Timer refreshJobConfigsFromConsulTimer;

        /// <summary>
        /// 指示从Consul刷新作业配置定时器的启用状态
        /// </summary>
        private CancellationTokenSource _refreshJobConfigsFromConfigCenterCts;

        /// <summary>
        /// 监控配置中心上的作业配置，及时更新到本地
        /// </summary>
        public void MonitorJobConfigsFromConfigCenter()
        {
            LogWriter.Write(string.Format("the timer of refresh job configs from the config center is starting ..."));

            if (_refreshJobConfigsFromConfigCenterCts == null
             || _refreshJobConfigsFromConfigCenterCts.IsCancellationRequested)
            {
                _refreshJobConfigsFromConfigCenterCts = new CancellationTokenSource();
            }

            StartRefreshJobConfigsFromConfigCenterTimer(_refreshJobConfigsFromConfigCenterCts.Token);
        }

        /// <summary>
        /// 停止监控配置中心上的作业配置
        /// </summary>
        public void StopMonitorJobConfigsFromConfigCenter()
        {
            if (_refreshJobConfigsFromConfigCenterCts != null &&
                !_refreshJobConfigsFromConfigCenterCts.IsCancellationRequested)
            {
                _refreshJobConfigsFromConfigCenterCts.Cancel();
            }

            StopRefreshJobConfigsFromConfigCenterTimer();

            LogWriter.Write(string.Format("the timer of refresh job configs from the config center has stopped"));
        }

        /// <summary>
        /// 开启从配置中心刷新作业配置Timer
        /// </summary>
        private void StartRefreshJobConfigsFromConfigCenterTimer(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            if (refreshJobConfigsFromConsulTimer == null)
            {
                refreshJobConfigsFromConsulTimer = new Timer(new TimerCallback(state => TimedRefreshJobConfigsFromConfigCenterCallback(state, cancellationToken)),
                     null, SwiftConfiguration.RefreshJobConfigsInterval, SwiftConfiguration.RefreshJobConfigsInterval);
            }
        }

        /// <summary>
        /// 关闭从配置中心刷新作业配置Timer
        /// </summary>
        private void StopRefreshJobConfigsFromConfigCenterTimer()
        {
            if (refreshJobConfigsFromConsulTimer != null)
            {
                refreshJobConfigsFromConsulTimer.Dispose();
                refreshJobConfigsFromConsulTimer = null;
            }
        }

        /// <summary>
        /// 从配置中心刷新作业配置
        /// </summary>
        /// <param name="state">State.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private void TimedRefreshJobConfigsFromConfigCenterCallback(object state, CancellationToken cancellationToken)
        {
            StopRefreshJobConfigsFromConfigCenterTimer();

            lock (jobConfigsLocker)
            {
                try
                {
                    var consulJobConfigs = GetJobConfigsFromConfigCenter(cancellationToken);
                    LogWriter.Write(string.Format("发现Consul作业配置数量:{0}", consulJobConfigs.Count), Log.LogLevel.Debug);

                    cancellationToken.ThrowIfCancellationRequested();

                    // 获取新增的JobConfig
                    List<JobConfig> newJobConfigList = GetNewJobConfigs(consulJobConfigs);

                    // 获取移除的JobConfig
                    List<JobConfig> removeJobConfigList = GetRemovedJobConfigs(consulJobConfigs);

                    // 更新JobConfig
                    ProcessChangedJobConfigs(consulJobConfigs, cancellationToken);

                    // 添加新JobConfig
                    ProcessNewJobConfigs(newJobConfigList, cancellationToken);

                    // 移除JobConfig
                    ProcessRemovedJobConfig(removeJobConfigList, cancellationToken);
                }
                catch (Exception ex)
                {
                    LogWriter.Write(string.Format("从Consul刷新作业配置异常:{0},{1}", ex.Message, ex.StackTrace));
                }
            }

            StartRefreshJobConfigsFromConfigCenterTimer(cancellationToken);
        }

        /// <summary>
        /// 获取被移除的作业配置
        /// </summary>
        /// <returns>The removed job configs.</returns>
        /// <param name="consulJobConfigs">Consul job configs.</param>
        private List<JobConfig> GetRemovedJobConfigs(List<JobConfig> consulJobConfigs, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            List<JobConfig> removedJobConfigList = new List<JobConfig>();
            foreach (var localJobConfig in jobConfigs)
            {
                var consulJobConfig = consulJobConfigs.FirstOrDefault(d => d.Name == localJobConfig.Name);
                if (consulJobConfig == null)
                {
                    removedJobConfigList.Add(localJobConfig);
                }
            }

            return removedJobConfigList;
        }

        /// <summary>
        /// 获取新添加的作业配置
        /// </summary>
        /// <returns>The new job configs.</returns>
        /// <param name="consulJobConfigs">Consul job configs.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private List<JobConfig> GetNewJobConfigs(List<JobConfig> consulJobConfigs, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            List<JobConfig> newJobConfigList = new List<JobConfig>();
            foreach (var consulJobConfig in consulJobConfigs)
            {
                var localJobConfig = jobConfigs.FirstOrDefault(d => d.Name == consulJobConfig.Name);
                if (localJobConfig == null)
                {
                    newJobConfigList.Add(consulJobConfig);
                }
            }

            return newJobConfigList;
        }

        /// <summary>
        /// 处理被移除的作业配置
        /// </summary>
        /// <param name="removeJobConfigList">Remove job config list.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private void ProcessRemovedJobConfig(List<JobConfig> removeJobConfigList, CancellationToken cancellationToken = default)
        {
            if (removeJobConfigList.Count > 0)
            {
                foreach (var removeJobConfig in removeJobConfigList)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var mRemoveResult = jobConfigs.Remove(removeJobConfig);
                    LogWriter.Write("已从内存移除作业配置[" + removeJobConfig.Name + "]，结果:" + mRemoveResult.ToString());

                    AsyncDoEventHandler(() =>
                    {
                        OnJobConfigRemoveEventHandler?.Invoke(removeJobConfig, cancellationToken);
                    }, cancellationToken);
                }
            }
        }

        /// <summary>
        /// 处理新添加的作业配置
        /// </summary>
        /// <param name="newJobConfigList">New job config list.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private void ProcessNewJobConfigs(List<JobConfig> newJobConfigList, CancellationToken cancellationToken = default)
        {
            foreach (var newJobConfig in newJobConfigList)
            {
                cancellationToken.ThrowIfCancellationRequested();

                jobConfigs.Add(newJobConfig);
                LogWriter.Write("已添加作业配置[" + newJobConfig.Name + "]到内存");

                AsyncDoEventHandler(() =>
                {
                    OnJobConfigJoinEventHandler?.Invoke(newJobConfig, cancellationToken);
                }, cancellationToken);
            }
        }

        /// <summary>
        /// 处理发生改变的作业配置
        /// </summary>
        /// <param name="consulJobConfigs">Consul job configs.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private void ProcessChangedJobConfigs(List<JobConfig> consulJobConfigs, CancellationToken cancellationToken = default)
        {
            for (int i = jobConfigs.Count - 1; i >= 0; i--)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var localJobConfig = jobConfigs[i];
                var consulJobConfig = consulJobConfigs.FirstOrDefault(d => d.Name == localJobConfig.Name);
                if (consulJobConfig != null)
                {
                    bool isNeedUpdate = false
                        || consulJobConfig.Version != localJobConfig.Version
                        || consulJobConfig.ModifyIndex != localJobConfig.ModifyIndex;

                    if (isNeedUpdate)
                    {
                        jobConfigs[i] = consulJobConfig;
                        LogWriter.Write("已更新作业配置[" + consulJobConfig.Name + "]到内存");

                        AsyncDoEventHandler(() =>
                        {
                            OnJobConfigUpdateEventHandler?.Invoke(consulJobConfig, cancellationToken);
                        }, cancellationToken);
                    }
                }
            }
        }

        /// <summary>
        /// 从配置中心获取集群的所有作业配置
        /// </summary>
        /// <returns></returns>
        private List<JobConfig> GetJobConfigsFromConfigCenter(CancellationToken cancellationToken = default)
        {
            List<JobConfig> consulJobConfigs = new List<JobConfig>();

            var jobConfigKeyPrefix = string.Format("Swift/{0}/Jobs", Name);
            var jobKeys = ConsulKV.Keys(jobConfigKeyPrefix, cancellationToken);
            var jobConfigKeys = jobKeys?.Where(d => d.EndsWith("Config", StringComparison.Ordinal));

            if (jobConfigKeys != null && jobConfigKeys.Any())
            {
                foreach (var jobConfigKey in jobConfigKeys)
                {
                    var jobKV = ConsulKV.Get(jobConfigKey, cancellationToken);
                    var jobJson = Encoding.UTF8.GetString(jobKV.Value);
                    var jobConfig = JobConfig.CreateInstance(jobJson);
                    jobConfig.ModifyIndex = jobKV.ModifyIndex;
                    consulJobConfigs.Add(jobConfig);
                }
            }

            return consulJobConfigs;
        }
        #endregion

        #region 作业记录
        /// <summary>
        /// 取消作业记录
        /// </summary>
        /// <returns><c>true</c>, if job record was canceled, <c>false</c> otherwise.</returns>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        public bool CancelJobRecord(string jobName, string jobId)
        {
            JobBase jobRecord = null;
            do
            {
                jobRecord = GetJobRecord(jobName, jobId);
                if (jobRecord == null)
                {
                    return true;
                }

                // 这些状态，作业都应该已经停止运行
                if (jobRecord.Status == EnumJobRecordStatus.Canceling
                || jobRecord.Status == EnumJobRecordStatus.Canceled
                || jobRecord.Status == EnumJobRecordStatus.TaskExecutingFailed
                || jobRecord.Status == EnumJobRecordStatus.PlanFailed
                || jobRecord.Status == EnumJobRecordStatus.TaskMerged
                || jobRecord.Status == EnumJobRecordStatus.TaskMergeFailed)
                {
                    return true;
                }

                // 更高作业和任务状态
                jobRecord.Status = EnumJobRecordStatus.Canceling;
                if (jobRecord.TaskPlan != null)
                {
                    var tasks = jobRecord.TaskPlan.SelectMany(d => d.Value);
                    foreach (var task in tasks)
                    {
                        task.Status = EnumTaskStatus.Canceling;
                    }
                }

            } while (!configCenter.CancelJobRecord(jobRecord));

            return true;
        }

        /// <summary>
        /// 获取作业记录
        /// </summary>
        /// <returns>The job record.</returns>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        public JobBase GetJobRecord(string jobName, string jobId)
        {
            return configCenter.GetJobRecord(jobName, jobId, this, default);
        }

        /// <summary>
        /// 获取最新的作业记录数组
        /// </summary>
        /// <returns>The current jobs.</returns>
        public JobBase[] GetLatestJobRecords(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            lock (activedJobsLocker)
            {
                try
                {
                    activedJobs = GetJobRecordsFromConfigCenter(cancellationToken);
                }
                catch (Exception ex)
                {
                    LogWriter.Write("从配置中心获取集群的最新作业记录异常", ex);
                }

                return activedJobs.ToArray();
            }
        }

        /// <summary>
        /// 从配置中心获取集群的所有最新作业记录
        /// </summary>
        /// <returns>The job records from config center.</returns>
        private List<JobBase> GetJobRecordsFromConfigCenter(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            List<JobBase> jobRecords = new List<JobBase>();
            var latestJobConfigs = GetLatestJobConfigs();

            // 遍历作业配置，看看对应的作业记录
            if (latestJobConfigs != null && latestJobConfigs.Length > 0)
            {
                for (int i = latestJobConfigs.Length - 1; i >= 0; i--)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var jobConfig = latestJobConfigs[i];
                    var jobRecord = ConfigCenter.GetJobRecord(jobConfig, this, cancellationToken);
                    if (jobRecord != null)
                    {
                        jobRecords.Add(jobRecord);
                    }
                }
            }

            return jobRecords;
        }
        #endregion

        #region 其它
        /// <summary>
        /// 获取本机联网IP
        /// </summary>
        /// <returns></returns>
        private string GetLocalIP()
        {
            return System.Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces()
            .Select(p => p.GetIPProperties())
            .SelectMany(p => p.UnicastAddresses)
            .FirstOrDefault(p => p.Address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork && !System.Net.IPAddress.IsLoopback(p.Address))?.Address.ToString();
        }

        /// <summary>
        /// 执行异步事件处理
        /// </summary>
        private void AsyncDoEventHandler(Action action, CancellationToken cancellationToken = default)
        {
            Task.Factory.StartNew(action, cancellationToken);
        }
        #endregion
    }
}
