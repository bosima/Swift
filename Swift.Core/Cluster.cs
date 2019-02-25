using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
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
    /// 作业加入事件
    /// </summary>
    /// <param name="job"></param>
    public delegate void JobRecordJoinEvent(JobBase job);

    /// <summary>
    /// 作业移除事件
    /// </summary>
    /// <param name="job"></param>
    public delegate void JobRecordRemoveEvent(JobBase job);

    /// <summary>
    /// 任务加入事件
    /// </summary>
    /// <param name="task"></param>
    public delegate void TaskJoinEvent(JobTask task);

    /// <summary>
    /// 任务移除事件
    /// </summary>
    /// <param name="task"></param>
    public delegate void TaskRemoveEvent(JobTask task);

    /// <summary>
    /// 作业配置加入事件
    /// </summary>
    /// <param name="jobConfig"></param>
    public delegate void JobConfigJoinEvent(JobConfig jobConfig);

    /// <summary>
    /// 作业配置移除事件
    /// </summary>
    /// <param name="jobConfig"></param>
    public delegate void JobConfigRemoveEvent(JobConfig jobConfig);

    /// <summary>
    /// 成员加入事件
    /// </summary>
    /// <param name="member"></param>
    public delegate void MemberJoinEvent(Member member);

    /// <summary>
    /// 成员移除事件
    /// </summary>
    /// <param name="member"></param>
    public delegate void MemberRemoveEvent(Member member);

    /// <summary>
    /// 集群
    /// </summary>
    public class Cluster
    {
        private readonly string name;
        private string localIP;
        private Manager manager;
        private Worker[] workers;
        private List<Member> members;
        private List<JobBase> activedJobs;
        private List<JobConfig> jobConfigs;
        private Member currentMember;
        private readonly object refreshLocker = new object();

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="name"></param>
        public Cluster(string name, string bindingIP)
        {
            this.name = name;
            this.localIP = bindingIP;
            this.members = new List<Member>();
            this.activedJobs = new List<JobBase>();
        }

        /// <summary>
        /// 作业加入事件
        /// </summary>
        public event JobRecordJoinEvent OnJobJoinEventHandler;

        /// <summary>
        /// 作业移除事件
        /// </summary>
        public event JobRecordRemoveEvent OnJobRemoveEventHandler;

        /// <summary>
        /// 作业配置加入事件。
        /// </summary>
        public event JobConfigJoinEvent OnJobConfigJoinEventHandler;

        /// <summary>
        /// 作业配置更新事件。
        /// </summary>
        public event JobConfigRemoveEvent OnJobConfigUpdateEventHandler;

        /// <summary>
        /// 作业配置移除事件。
        /// </summary>
        public event JobConfigRemoveEvent OnJobConfigRemoveEventHandler;

        /// <summary>
        /// 成员加入事件
        /// </summary>
        public event MemberJoinEvent OnMemberJoinEventHandler;

        /// <summary>
        /// 成员移除事件
        /// </summary>
        public event MemberRemoveEvent OnMemberRemoveEventHandler;

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
        /// 获取管理员
        /// </summary>
        public Manager Manager
        {
            get
            {
                lock (members)
                {
                    return manager;
                }
            }
        }

        /// <summary>
        /// 获取当前注册的工人数组
        /// </summary>
        /// <returns>The current workers.</returns>
        public Worker[] GetCurrentWorkers()
        {
            lock (members)
            {
                return members.Where(d => d.Role == EnumMemberRole.Worker).Select(d => (Worker)d).ToArray();
            }
        }

        /// <summary>
        /// 获取当前的作业数组
        /// </summary>
        /// <returns>The current jobs.</returns>
        public JobBase[] GetCurrentJobs()
        {
            lock (refreshLocker)
            {
                RefreshJobs();

                JobBase[] jobs = new JobBase[activedJobs.Count];
                if (activedJobs.Count > 0)
                {
                    activedJobs.CopyTo(jobs);
                }
                return jobs;
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
        /// 注册为Manager
        /// </summary>
        /// <returns></returns>
        public Member RegisterManager(string memberId)
        {
            Manager member = new Manager()
            {
                Id = memberId,
                Role = EnumMemberRole.Manager,

                OnlineTime = DateTime.Now,
                Status = 1,
            };

            return RegisterMember(member);
        }

        /// <summary>
        /// 注册为Worker
        /// </summary>
        /// <param name="memberId"></param>
        /// <returns></returns>
        public Member RegisterWorker(string memberId)
        {
            Worker member = new Worker()
            {
                Id = memberId,
                Role = EnumMemberRole.Worker,
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
                    while (!TryRegisterMemberToConsul(member, out latestMembers))
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
        private bool TryRegisterMemberToConsul(Member member, out List<Member> latestMemberList)
        {
            // todo:改成仅注册为成员，由成员来选举manager，这样能够解决固定manager挂掉的问题

            latestMemberList = null;

            List<Member> memberList = GetMembersFromConsul();

            if (member.Role == EnumMemberRole.Manager)
            {
                var currentManager = memberList.FirstOrDefault(d => d.Role == EnumMemberRole.Manager);
                if (currentManager != null && currentManager.Id != member.Id)
                {
                    throw new Exception(string.Format("不能注册为Manager，已经存在:{0}", Manager.Id));
                }
            }

            Member historyMember = memberList.FirstOrDefault(d => d.Id == member.Id);
            if (historyMember != null)
            {
                historyMember.Status = 1;
                historyMember.Role = member.Role;
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
            ConsulService.RegisterService(serviceId, serviceName, 10);
        }
        #endregion

        #region 监控成员变化
        /// <summary>
        /// 成员刷新定时器
        /// </summary>
        private Timer refreshMemberTimer;

        /// <summary>
        /// 监控成员变化
        /// </summary>
        public void MonitorMembers()
        {
            LogWriter.Write("开启监控集群成员变化...");

            StartRefreshMembersTimer();
        }

        /// <summary>
        /// 刷新成员
        /// </summary>
        private void TimedRefreshMembersCallback(object state)
        {
            StopRefreshMembersTimer();

            LogWriter.Write("开始刷新内存集群成员...");

            lock (members)
            {
                try
                {
                    var consulMembers = GetMembersFromConsul();

                    // 更新成员状态
                    foreach (var memoryMember in members)
                    {
                        var consulMember = consulMembers.FirstOrDefault(d => d.Id == memoryMember.Id);
                        if (consulMember != null)
                        {
                            memoryMember.Role = consulMember.Role;
                            memoryMember.Status = consulMember.Status;
                            memoryMember.OnlineTime = consulMember.OnlineTime;
                            memoryMember.OfflineTime = consulMember.OfflineTime;
                        }
                    }

                    // 获取新增的成员
                    List<Member> newMemberList = new List<Member>();
                    foreach (var consulMember in consulMembers)
                    {
                        if (!members.Any(d => d.Id == consulMember.Id))
                        {
                            newMemberList.Add(consulMember);
                        }
                    }

                    // 获取移除的成员
                    List<Member> removeMemberList = new List<Member>();
                    foreach (var member in members)
                    {
                        if (!consulMembers.Any(d => d.Id == member.Id))
                        {
                            removeMemberList.Add(member);
                        }
                    }

                    // 添加新成员
                    if (newMemberList.Count > 0)
                    {
                        foreach (var newMember in newMemberList)
                        {
                            members.Add(newMember);
                            LogWriter.Write(string.Format("已添加成员:{0},{1}", newMember.Id, newMember.Role.ToString()));

                            AsyncDoEventHandler(() =>
                            {
                                OnMemberJoinEventHandler?.Invoke(newMember);
                            });
                        }
                    }

                    // 移除删除的成员
                    if (removeMemberList.Count > 0)
                    {
                        foreach (var removeMember in removeMemberList)
                        {
                            members.Remove(removeMember);
                            LogWriter.Write(string.Format("已移除成员:{0},{1}", removeMember.Id, removeMember.Role.ToString()));

                            AsyncDoEventHandler(() =>
                            {
                                OnMemberRemoveEventHandler?.Invoke(removeMember);
                            });
                        }
                    }

                    manager = members.Where(d => d.Role == EnumMemberRole.Manager).Select(d => (Manager)d).FirstOrDefault();
                }
                catch (Exception ex)
                {
                    LogWriter.Write(string.Format("刷新内存集群成员异常:{0},{1}", ex.Message, ex.StackTrace));
                    return;
                }
            }

            LogWriter.Write("结束刷新内存集群成员。");

            StartRefreshMembersTimer();
        }

        /// <summary>
        /// 从Consul获取集群成员
        /// </summary>
        private List<Member> GetMembersFromConsul()
        {
            var memberKey = string.Format("Swift/{0}/Members", Name);
            KVPair memberKV = ConsulKV.Get(memberKey);
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
                Member member = null;
                if (configMember.Role == EnumMemberRole.Manager)
                {
                    member = configMember.ConvertTo<Manager>();
                }
                else if (configMember.Role == EnumMemberRole.Worker)
                {
                    member = configMember.ConvertTo<Worker>();
                }
                member.Cluster = this;
                memberList.Add(member);
            }

            return memberList;
        }

        /// <summary>
        /// 开启成员刷新Timer
        /// </summary>
        private void StartRefreshMembersTimer()
        {
            if (refreshMemberTimer == null)
            {
                refreshMemberTimer = new Timer(new TimerCallback(TimedRefreshMembersCallback), null, SwiftConfiguration.RefreshMemberInterval, SwiftConfiguration.RefreshMemberInterval);
            }
        }

        /// <summary>
        /// 关闭成员刷新Timer
        /// </summary>
        private void StopRefreshMembersTimer()
        {
            if (refreshMemberTimer != null)
            {
                refreshMemberTimer.Dispose();
                refreshMemberTimer = null;
            }
        }
        #endregion

        #region 监控成员健康状况

        /// <summary>
        /// 检查成员状态定时器
        /// </summary>
        private Timer checkMembersTimer;

        /// <summary>
        /// 监控成员健康状况
        /// </summary>
        public void MonitorMembersHealth()
        {
            LogWriter.Write("开启集群成员健康检查...");

            StartCheckMembersTimer();
        }

        /// <summary>
        /// 定时检查成员状态回调
        /// </summary>
        /// <param name="state">State.</param>
        private void TimedCheckMembersCallback(object state)
        {
            StopCheckMembersTimer();

            try
            {
                LogWriter.Write("定时成员健康检查开始执行...");

                // 检查成员健康状态，如果成员健康状态变化则尝试更新到Consul KV，直到成员状态和Consul KV中一致。
                while (!CheckMemberHealth())
                {
                    LogWriter.Write("更新成员健康状态到Consul失败，Consul数据已改变，稍后重试");
                    Thread.Sleep(300);
                }

                LogWriter.Write("定时成员健康检查执行完毕。");
            }
            catch (Exception ex)
            {
                LogWriter.Write("定时成员健康检查异常。", ex);
            }

            StartCheckMembersTimer();
        }

        /// <summary>
        /// 检查成员健康状态，如果改变则尝试更新到Consul。
        /// 通过Consul健康检查获取健康状态，并且仅更新到Consul KV，不更新本地内存缓存节点健康状态。
        /// 本地内存缓存节点健康状态需要另一个定时器来更新。
        /// </summary>
        /// <returns><c>true</c>, if member health was checked, <c>false</c> otherwise.</returns>
        private bool CheckMemberHealth()
        {
            // 从Consul获取所有成员
            var memberKey = string.Format("Swift/{0}/Members", Name);
            KVPair memberKV = ConsulKV.Get(memberKey);
            if (memberKV == null)
            {
                memberKV = ConsulKV.Create(memberKey);
            }

            var configMemberList = new List<MemberWrapper>();
            if (memberKV.Value != null)
            {
                configMemberList = JsonConvert.DeserializeObject<List<MemberWrapper>>(Encoding.UTF8.GetString(memberKV.Value));
            }

            bool isNeedUpdateConfig = false;

            // 检查成员的健康状态
            var serviceName = string.Format("Swift-{0}-Member", Name);
            var healths = ConsulService.GetHealths(serviceName);
            List<MemberWrapper> needRemoveList = new List<MemberWrapper>();
            foreach (var configMember in configMemberList)
            {
                var oldStatus = configMember.Status;
                var serviceId = string.Format("Swift-{0}-Member-{1}", Name, configMember.Id);
                healths.TryGetValue(serviceId, out bool isHealth);
                configMember.Status = (isHealth ? 1 : 0);
                LogWriter.Write(string.Format("成员新旧健康状态:{0},{1}->{2}", serviceId, oldStatus, configMember.Status));
                if (configMember.Status != oldStatus)
                {
                    // 变健康了，更改上线时间
                    if (configMember.Status == 1)
                    {
                        isNeedUpdateConfig = true;
                        configMember.OnlineTime = DateTime.Now;
                    }

                    // 变不好了，更改下线时间或者移除节点
                    if (configMember.Status == 0)
                    {
                        if (!configMember.OfflineTime.HasValue
                        || configMember.OnlineTime > configMember.OfflineTime)
                        {
                            isNeedUpdateConfig = true;
                            configMember.OfflineTime = DateTime.Now;
                        }
                        else
                        {
                            // 离线超过30分钟了，移除成员
                            if (DateTime.Now.Subtract(configMember.OfflineTime.Value).TotalMinutes > 30)
                            {
                                isNeedUpdateConfig = true;
                                needRemoveList.Add(configMember);
                            }
                        }
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

            // 更新到Consul中
            if (isNeedUpdateConfig)
            {
                LogWriter.Write("成员健康状态出现变化，将要更新到Consul...");
                memberKV.Value = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(configMemberList));
                return ConsulKV.CAS(memberKV);
            }

            LogWriter.Write("成员健康状态没有变化。");

            return true;
        }

        /// <summary>
        /// 开启成员健康检查Timer
        /// </summary>
        private void StartCheckMembersTimer()
        {
            if (checkMembersTimer == null)
            {
                checkMembersTimer = new Timer(new TimerCallback(TimedCheckMembersCallback), null, SwiftConfiguration.CheckMemberInterval, SwiftConfiguration.CheckMemberInterval);
            }
        }

        /// <summary>
        /// 关闭成员健康检查Timer
        /// </summary>
        private void StopCheckMembersTimer()
        {
            checkMembersTimer.Dispose();
            checkMembersTimer = null;
        }
        #endregion

        #region 作业
        /// <summary>
        /// 作业刷新定时器
        /// </summary>
        private Timer refreshJobsTimer;

        /// <summary>
        /// 监控作业变化
        /// </summary>
        public void MonitorJobs()
        {
            LogWriter.Write("开启定时刷新作业...");

            StartRefreshJobsTimer();
        }

        /// <summary>
        /// 刷新作业
        /// </summary>
        /// <param name="state"></param>
        public void TimedRefreshJobsCallback(object state)
        {
            StopRefreshJobsTimer();

            lock (refreshLocker)
            {
                RefreshJobs();
            }

            StartRefreshJobsTimer();
        }

        /// <summary>
        /// 执行刷新作业操作
        /// </summary>
        private void RefreshJobs()
        {
            LogWriter.Write("开始刷新作业列表...");

            try
            {
                // 遍历作业配置，看看对应的作业记录
                if (jobConfigs != null && jobConfigs.Count > 0)
                {
                    for (int i = jobConfigs.Count - 1; i >= 0; i--)
                    {
                        var jobConfig = jobConfigs[i];

                        // 内存中对应的作业
                        var currentJob = activedJobs.FirstOrDefault(d => d.Name == jobConfig.Name);

                        // 作业配置有最新的作业记录
                        if (!string.IsNullOrWhiteSpace(jobConfig.LastRecordId))
                        {
                            // 如果此时内存中还没有，则直接添加到内存
                            if (currentJob == null)
                            {
                                try
                                {
                                    AddActivedJobRecord(jobConfig);
                                }
                                catch (Exception ex)
                                {
                                    LogWriter.Write("添加作业记录到内存异常:" + ex.Message, Log.LogLevel.Error);
                                }

                                continue;
                            }

                            // 如果内存中的作业记录Id不是最新的作记录Id
                            if (currentJob.Id != jobConfig.LastRecordId)
                            {
                                // 先移除旧的作业记录
                                RemoveActivedJobRecord(currentJob);

                                try
                                {
                                    // 再添加新的作业记录
                                    AddActivedJobRecord(jobConfig);
                                }
                                catch (Exception ex)
                                {
                                    LogWriter.Write("添加作业到内存异常:" + ex.Message, Log.LogLevel.Error);
                                }
                            }
                            else
                            {
                                UpdateActivedJobRecord(currentJob, jobConfig);
                            }
                        }
                    }

                }

                // 去掉已经从Consul中移除的作业
                for (int i = activedJobs.Count - 1; i >= 0; i--)
                {
                    var job = activedJobs[i];
                    if (!jobConfigs.Any(d => d.Name == job.Name))
                    {
                        activedJobs.Remove(job);
                        LogWriter.Write(string.Format("已从内存移除作业记录:{0},{1}", job.Name, job.Id));
                    }
                }

                LogWriter.Write("结束刷新作业列表。");
            }
            catch (Exception ex)
            {
                LogWriter.Write(string.Format("刷新作业记录异常:{0}", ex.Message));
            }
        }

        /// <summary>
        /// 添加活动作业记录
        /// </summary>
        /// <param name="jobConfig">作业记录对应的作业配置</param>
        private void AddActivedJobRecord(JobConfig jobConfig)
        {
            var jobRecordKey = string.Format("Swift/{0}/Jobs/{1}/Records/{2}", Name, jobConfig.Name, jobConfig.LastRecordId);
            var jobRecordKV = ConsulKV.Get(jobRecordKey);
            if (jobRecordKV == null || jobRecordKV.Value == null)
            {
                throw new Exception("Consul中作业[" + jobConfig.Name + "]的记录[" + jobConfig.LastRecordId + "]配置丢失");
            }

            var jobRecord = JobBase.Deserialize(Encoding.UTF8.GetString(jobRecordKV.Value), this);
            jobRecord.ModifyIndex = jobRecordKV.ModifyIndex;
            activedJobs.Add(jobRecord);
            LogWriter.Write(string.Format("已添加新作业记录到本地内存:{0},{1}", jobConfig.Name, jobRecord.Id));

            AsyncDoEventHandler(() =>
            {
                OnJobJoinEventHandler?.Invoke(jobRecord);
            });
        }

        /// <summary>
        /// 移除活动作业记录
        /// </summary>
        /// <param name="job"></param>
        private void RemoveActivedJobRecord(JobBase job)
        {
            activedJobs.Remove(job);
            LogWriter.Write(string.Format("已从内存移除作业记录:{0},{1}", job.Name, job.Id));

            AsyncDoEventHandler(() =>
            {
                OnJobRemoveEventHandler?.Invoke(job);
            });
        }

        /// <summary>
        /// 更新活动作业记录
        /// </summary>
        /// <param name="job"></param>
        private void UpdateActivedJobRecord(JobBase job, JobConfig jobConfig)
        {
            lock (string.Intern(string.Format("jobrecord:{0},{1}", job.Name, job.Id)))
            {
                var jobRecordKey = string.Format("Swift/{0}/Jobs/{1}/Records/{2}", Name, jobConfig.Name, jobConfig.LastRecordId);
                var jobRecordKV = ConsulKV.Get(jobRecordKey);

                // 本地的ModifyIndex小于从Consul获取的ModifyIndex才更新，避免本地未提交的处理被覆盖
                if (job.ModifyIndex < jobRecordKV.ModifyIndex)
                {
                    LogWriter.Write(string.Format("作业[{0}]本地活跃记录需要更新:{1},{2}", jobConfig.Name, job.ModifyIndex, jobRecordKV.ModifyIndex), Log.LogLevel.Trace);

                    var jobRecord = JobBase.Deserialize(Encoding.UTF8.GetString(jobRecordKV.Value), this);
                    jobRecord.ModifyIndex = jobRecordKV.ModifyIndex;
                    job.UpdateFrom(jobRecord);
                    LogWriter.Write(string.Format("已在本地内存更新作业记录:{0},{1},{2}", jobConfig.Name, jobConfig.LastRecordId, job.ModifyIndex));
                }
            }
        }

        /// <summary>
        /// 开启成员健康检查Timer
        /// </summary>
        private void StartRefreshJobsTimer()
        {
            if (refreshJobsTimer == null)
            {
                refreshJobsTimer = new Timer(new TimerCallback(TimedRefreshJobsCallback), null, SwiftConfiguration.RefreshJobInterval, SwiftConfiguration.RefreshJobInterval);
            }
        }

        /// <summary>
        /// 关闭成员健康检查Timer
        /// </summary>
        private void StopRefreshJobsTimer()
        {
            refreshJobsTimer.Dispose();
            refreshJobsTimer = null;
        }
        #endregion

        #region 监控本地磁盘作业配置
        /// <summary>
        /// 本地磁盘作业配置刷新定时器
        /// </summary>
        private Timer refreshJobConfigFromDiskTimer;

        /// <summary>
        /// 监控本地磁盘作业配置变化
        /// </summary>
        public void MonitorJobConfigsFromDisk()
        {
            LogWriter.Write(string.Format("开启监控本地磁盘作业配置变化..."));
            StartRefreshJobConfigFromDiskTimer();
        }

        /// <summary>
        /// 刷新本地磁盘作业配置
        /// </summary>
        private void TimedRefreshJobConfigsFromDiskCallback(object state)
        {
            // TODO:将配置中心抽象为接口，Consul KV只是一种实现

            StopRefreshJobConfigFromDiskTimer();

            lock (refreshLocker)
            {
                try
                {
                    var diskJobConfigs = LoadJobConfigsFromDisk();
                    var consulJobConfigs = GetJobConfigsFromConsul();

                    LogWriter.Write(string.Format("本地磁盘作业配置数量:{0}", diskJobConfigs.Count));
                    LogWriter.Write(string.Format("配置中心作业配置数量:{0}", consulJobConfigs.Count));

                    foreach (var diskJobConfig in diskJobConfigs)
                    {
                        LogWriter.Write("开始检查作业配置:" + diskJobConfig.Name);
                        var setResult = AddOrUpdateJobConfigToConsul(diskJobConfig);
                        LogWriter.Write("已处理作业配置:" + diskJobConfig.Name + "," + setResult);
                    }

                    List<JobConfig> removeJobConfigList = new List<JobConfig>();
                    foreach (var consulJobConfig in consulJobConfigs)
                    {
                        if (!diskJobConfigs.Any(d => d.Name == consulJobConfig.Name))
                        {
                            var configRemoveResult = RemoveJobConfigFromConsul(consulJobConfig);
                            LogWriter.Write("移除配置中心作业配置:" + consulJobConfig.Name + "," + configRemoveResult);
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogWriter.Write(string.Format("刷新本地磁盘作业配置到配置中心异常:{0}", ex.Message));
                }
            }

            StartRefreshJobConfigFromDiskTimer();
        }

        /// <summary>
        /// 从磁盘加载作业配置
        /// </summary>
        /// <returns></returns>
        private List<JobConfig> LoadJobConfigsFromDisk()
        {
            List<JobConfig> latestJobConfigs = new List<JobConfig>();

            var jobRootPath = Path.Combine(SwiftConfiguration.BaseDirectory, "Jobs");
            if (!Directory.Exists(jobRootPath))
            {
                LogWriter.Write(string.Format("作业包目录为空，不能再轻松了。"));
                return latestJobConfigs;
            }

            // 查找zip作业包
            var jobPkgPaths = Directory.GetFiles(jobRootPath, "*.zip");
            foreach (var pkgPath in jobPkgPaths)
            {
                // 确认作业配置文件
                JobConfig jobConfig = EnsureJobConfigFileIsLatest(pkgPath);
                latestJobConfigs.Add(jobConfig);
            }

            return latestJobConfigs;
        }

        /// <summary>
        /// 确保作业配置文件是最新的版本
        /// </summary>
        private static JobConfig EnsureJobConfigFileIsLatest(string pkgPath)
        {
            var jobPkgLockName = SwiftConfiguration.GetFileOperateLockName(pkgPath);

            lock (string.Intern(jobPkgLockName))
            {
                // 当前作业包更新时间
                var pkgUpdateTime = File.GetLastWriteTime(pkgPath);

                // 当前作业包版本
                var pkgVersion = pkgUpdateTime.ToString("yyyyMMddHHmmss");

                // 作业根目录
                var jobRootPath = Path.Combine(SwiftConfiguration.BaseDirectory, "Jobs");

                // 作业名称
                int fileNameStartIndex = pkgPath.LastIndexOf(Path.DirectorySeparatorChar) + 1;
                string pkgName = pkgPath.Substring(fileNameStartIndex, pkgPath.LastIndexOf('.') - fileNameStartIndex);

                // 作业配置文件路径
                var jobConfigDirectory = Path.Combine(jobRootPath, pkgName, "config");

                // 从作业包提取配置文件，如果已经存在则忽略
                ExtractJobConfigFile(pkgPath, jobConfigDirectory);

                // 反序列化作业配置文件
                string jobConfigPath = Path.Combine(jobConfigDirectory, "job.json");
                JobConfig jobConfig = new JobConfig(jobConfigPath);

                // 如果是新的作业包，设置作业包版本
                if (string.IsNullOrWhiteSpace(jobConfig.Version))
                {
                    jobConfig.Version = pkgVersion;
                    UpdateJobConfigFile(jobConfigPath, jobConfig);
                }

                // 如果作业包版本改变，更新作业包配置
                if (!string.IsNullOrWhiteSpace(jobConfig.Version)
                    && jobConfig.Version != pkgVersion)
                {
                    // 从作业包解压获取最新的配置文件
                    ExtractJobConfigFile(pkgPath, jobConfigDirectory, true);

                    // 设置配置文件的动态信息
                    var latestJobConfig = new JobConfig(jobConfigPath)
                    {
                        LastRecordId = jobConfig.LastRecordId,
                        LastRecordStartTime = jobConfig.LastRecordStartTime,
                        ModifyIndex = jobConfig.ModifyIndex,
                        Version = pkgVersion
                    };
                    UpdateJobConfigFile(jobConfigPath, latestJobConfig);

                    jobConfig = latestJobConfig;
                }

                return jobConfig;
            }
        }

        /// <summary>
        /// 更新作业配置文件
        /// </summary>
        /// <param name="configPath"></param>
        /// <param name="config"></param>
        private static void UpdateJobConfigFile(string configPath, JobConfig config)
        {
            File.WriteAllText(configPath, JsonConvert.SerializeObject(config));
        }

        /// <summary>
        /// 提取作业配置文件
        /// </summary>
        /// <param name="pkgPath">作业包文件路径</param>
        /// <param name="configFolderPath">作业配置文件夹路径</param>
        /// <param name="isCover">是否覆盖，作业配置文件可能已存在，如果允许覆盖，则替换作业配置文件，默认不允许覆盖</param>
        private static void ExtractJobConfigFile(string pkgPath, string configFolderPath, bool isCover = false)
        {
            var filePath = Path.Combine(configFolderPath, "job.json");

            if (isCover || !File.Exists(filePath))
            {
                if (!Directory.Exists(configFolderPath))
                {
                    Directory.CreateDirectory(configFolderPath);
                }

                try
                {
                    using (var zip = ZipFile.Open(pkgPath, ZipArchiveMode.Read))
                    {
                        zip.GetEntry("job.json").ExtractToFile(filePath, true);
                    }
                }
                catch (Exception ex)
                {
                    throw new JobPackageConfigExtractException(string.Format("作业包[{0}]提取配置文件异常", pkgPath), ex);
                }
            }
        }

        /// <summary>
        /// 添加或更新作业配置到Conusl
        /// </summary>
        /// <param name="jobConfig"></param>
        /// <returns></returns>
        private bool AddOrUpdateJobConfigToConsul(JobConfig jobConfig)
        {
            KVPair configKV = null;

            do
            {
                var jobConfigKey = string.Format("Swift/{0}/Jobs/{1}/Config", Name, jobConfig.Name);
                configKV = ConsulKV.Get(jobConfigKey);
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
                    jobConfig.LastRecordStartTime = oldJobConfig.LastRecordStartTime;
                    jobConfig.ModifyIndex = oldJobConfig.ModifyIndex;
                }

                configKV.Value = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(jobConfig));

            } while (!ConsulKV.CAS(configKV));

            return true;
        }

        /// <summary>
        /// 移除作业配置，成功返回true，否则返回false
        /// </summary>
        /// <param name="jobConfig"></param>
        /// <returns></returns>
        private bool RemoveJobConfigFromConsul(JobConfig jobConfig)
        {
            var jobConfigKey = string.Format("Swift/{0}/Jobs/{1}", Name, jobConfig.Name);
            return ConsulKV.DeleteTree(jobConfigKey);
        }

        /// <summary>
        /// 开启成员健康检查Timer
        /// </summary>
        private void StartRefreshJobConfigFromDiskTimer()
        {
            if (refreshJobConfigFromDiskTimer == null)
            {
                refreshJobConfigFromDiskTimer = new Timer(new TimerCallback(TimedRefreshJobConfigsFromDiskCallback), null, SwiftConfiguration.RefreshJobConfigInterval, SwiftConfiguration.RefreshJobConfigInterval);
            }
        }

        /// <summary>
        /// 关闭成员健康检查Timer
        /// </summary>
        private void StopRefreshJobConfigFromDiskTimer()
        {
            refreshJobConfigFromDiskTimer.Dispose();
            refreshJobConfigFromDiskTimer = null;
        }
        #endregion

        #region 监控作业创建
        /// <summary>
        /// 作业创建定时器
        /// </summary>
        private Timer jobCreateTimer;

        /// <summary>
        /// 监控作业记录创建
        /// </summary>
        public void MonitorJobCreate()
        {
            LogWriter.Write(string.Format("开启定时创建作业..."));
            StartCreateJobTimer();
        }

        /// <summary>
        /// 定时创建作业
        /// </summary>
        /// <param name="state"></param>
        private void TimedCreateJobCallback(object state)
        {
            StopCreateJobTimer();

            lock (refreshLocker)
            {
                if (jobConfigs != null && jobConfigs.Count > 0)
                {
                    LogWriter.Write(string.Format("开始定时创建作业检查..."));

                    foreach (var jobConfig in jobConfigs)
                    {
                        // 检查作业配置的上一次作业记录状态，如果未完成则不能创建新的作业记录
                        if (!string.IsNullOrWhiteSpace(jobConfig.LastRecordId))
                        {
                            var lastJobRecordKey = string.Format("Swift/{0}/Jobs/{1}/Records/{2}", Name, jobConfig.Name, jobConfig.LastRecordId);
                            var lastJobRecordKV = ConsulKV.Get(lastJobRecordKey);
                            if (lastJobRecordKV != null)
                            {
                                var lastJobRecord = JsonConvert.DeserializeObject<JobWrapper>(Encoding.UTF8.GetString(lastJobRecordKV.Value));
                                if (lastJobRecord.Status != EnumJobRecordStatus.TaskMerged)
                                {
                                    LogWriter.Write(string.Format("上一次作业记录未完成:{0},{1},{2}", jobConfig.Name, jobConfig.LastRecordId, lastJobRecord.Status));
                                    continue;
                                }
                            }
                        }

                        if (jobConfig.RunTimes != null && jobConfig.RunTimes.Length > 0)
                        {
                            foreach (var timePlan in jobConfig.RunTimes)
                            {
                                if (timePlan.CheckIsTime(jobConfig.LastRecordStartTime))
                                {
                                    // 如果作业没有创建，则创建作业，同时更新作业配置
                                    var job = JobBase.CreateInstance(jobConfig, this);
                                    var jobKey = string.Format("Swift/{0}/Jobs/{1}/Records/{2}", Name, jobConfig.Name, job.Id);
                                    var jobKV = ConsulKV.Get(jobKey);

                                    if (jobKV == null)
                                    {
                                        // 创建作业
                                        jobKV = ConsulKV.Create(jobKey);
                                        jobKV.Value = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(job));
                                        ConsulKV.CAS(jobKV);
                                        LogWriter.Write(string.Format("已创建作业:{0},{1}", jobConfig.Name, job.Id));

                                        // 更新Consul作业配置
                                        var jobConfigKey = string.Format("Swift/{0}/Jobs/{1}/Config", Name, jobConfig.Name);
                                        var jobConfigKV = ConsulKV.Get(jobConfigKey);
                                        if (jobConfigKV == null)
                                        {
                                            // 可能Consul中找不到了
                                            jobConfigKV = ConsulKV.Create(jobConfigKey);
                                        }

                                        jobConfig.LastRecordId = job.Id;
                                        jobConfig.LastRecordStartTime = DateTime.Now;
                                        jobConfig.ModifyIndex = jobConfigKV.ModifyIndex;
                                        var jobConfigJson = JsonConvert.SerializeObject(jobConfig);
                                        jobConfigKV.Value = Encoding.UTF8.GetBytes(jobConfigJson);
                                        ConsulKV.CAS(jobConfigKV);

                                        // 更新本地作业配置
                                        var jobConfigLocalPath = Path.Combine(SwiftConfiguration.BaseDirectory, "Jobs", jobConfig.Name, "config", "job.json");
                                        File.WriteAllText(jobConfigLocalPath, jobConfigJson);

                                        LogWriter.Write(string.Format("已更新作业配置:{0}", jobConfig.Name));
                                    }
                                }
                            }
                        }
                    }

                    LogWriter.Write(string.Format("结束定时创建作业检查。"));
                }
            }

            StartCreateJobTimer();
        }

        /// <summary>
        /// 开启作业创建Timer
        /// </summary>
        private void StartCreateJobTimer()
        {
            if (jobCreateTimer == null)
            {
                jobCreateTimer = new Timer(new TimerCallback(TimedCreateJobCallback), null, SwiftConfiguration.JobSpaceCreateInterval, SwiftConfiguration.JobSpaceCreateInterval);
            }
        }

        /// <summary>
        /// 关闭作业创建Timer
        /// </summary>
        private void StopCreateJobTimer()
        {
            jobCreateTimer.Dispose();
            jobCreateTimer = null;
        }
        #endregion

        #region 监控Consul作业配置
        /// <summary>
        /// 从Consul刷新作业配置定时器
        /// </summary>
        private Timer refreshJobConfigFromConsulTimer;

        /// <summary>
        /// 监控Consul上的作业配置，及时更新到本地
        /// </summary>
        public void MonitorJobConfigsFromConsul()
        {
            StartRefreshJobConfigFromConsulTimer();
        }

        /// <summary>
        /// 开启从Consul刷新作业配置Timer
        /// </summary>
        private void StartRefreshJobConfigFromConsulTimer()
        {
            if (refreshJobConfigFromConsulTimer == null)
            {
                refreshJobConfigFromConsulTimer = new Timer(new TimerCallback(TimedRefreshJobConfigsFromConsulCallback), null, SwiftConfiguration.RefreshJobConfigInterval, SwiftConfiguration.RefreshJobConfigInterval);
            }
        }

        /// <summary>
        /// 关闭从Consul刷新作业配置Timer
        /// </summary>
        private void StopRefreshJobConfigFromConsulTimer()
        {
            refreshJobConfigFromConsulTimer.Dispose();
            refreshJobConfigFromConsulTimer = null;
        }

        /// <summary>
        /// 通过Consul刷新作业配置
        /// </summary>
        /// <param name="state"></param>
        private void TimedRefreshJobConfigsFromConsulCallback(object state)
        {
            StopRefreshJobConfigFromConsulTimer();

            lock (refreshLocker)
            {
                try
                {
                    var consulJobConfigs = GetJobConfigsFromConsul();
                    LogWriter.Write(string.Format("发现Consul作业配置数量:{0}", consulJobConfigs.Count));

                    if (jobConfigs == null)
                    {
                        jobConfigs = new List<JobConfig>();
                    }

                    // 获取新增的JobConfig
                    List<JobConfig> newJobConfigList = new List<JobConfig>();
                    foreach (var consulJobConfig in consulJobConfigs)
                    {
                        var localJobConfig = jobConfigs.FirstOrDefault(d => d.Name == consulJobConfig.Name);
                        if (localJobConfig == null)
                        {
                            newJobConfigList.Add(consulJobConfig);
                        }
                    }

                    // 获取移除的JobConfig
                    List<JobConfig> removeJobConfigList = new List<JobConfig>();
                    foreach (var localJobConfig in jobConfigs)
                    {
                        var consulJobConfig = consulJobConfigs.FirstOrDefault(d => d.Name == localJobConfig.Name);
                        if (consulJobConfig == null)
                        {
                            removeJobConfigList.Add(localJobConfig);
                        }
                    }

                    // 更新JobConfig
                    for (int i = jobConfigs.Count - 1; i >= 0; i--)
                    {
                        var localJobConfig = jobConfigs[i];
                        var consulJobConfig = consulJobConfigs.FirstOrDefault(d => d.Name == localJobConfig.Name);
                        if (consulJobConfig != null)
                        {
                            bool isNeedUpdate = false || consulJobConfig.Version != localJobConfig.Version;

                            if (isNeedUpdate || consulJobConfig.ModifyIndex != localJobConfig.ModifyIndex)
                            {
                                jobConfigs[i] = consulJobConfig;
                                LogWriter.Write("已更新作业配置[" + consulJobConfig.Name + "]到内存");
                            }

                            if (isNeedUpdate)
                            {
                                AsyncDoEventHandler(() =>
                                {
                                    OnJobConfigUpdateEventHandler?.Invoke(consulJobConfig);
                                });
                            }
                        }
                    }

                    // 添加新JobConfig
                    if (newJobConfigList.Count > 0)
                    {
                        foreach (var newJobConfig in newJobConfigList)
                        {
                            jobConfigs.Add(newJobConfig);
                            LogWriter.Write("已添加作业配置[" + newJobConfig.Name + "]到内存");

                            AsyncDoEventHandler(() =>
                            {
                                OnJobConfigJoinEventHandler?.Invoke(newJobConfig);
                            });
                        }
                    }

                    // 移除删除的JobConfig
                    if (removeJobConfigList.Count > 0)
                    {
                        foreach (var removeJobConfig in removeJobConfigList)
                        {
                            var mRemoveResult = jobConfigs.Remove(removeJobConfig);
                            LogWriter.Write("已从内存移除作业配置[" + removeJobConfig.Name + "]，结果:" + mRemoveResult.ToString());

                            AsyncDoEventHandler(() =>
                            {
                                OnJobConfigRemoveEventHandler?.Invoke(removeJobConfig);
                            });
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogWriter.Write(string.Format("从Consul刷新作业配置异常:{0},{1}", ex.Message, ex.StackTrace));
                }
            }

            StartRefreshJobConfigFromConsulTimer();
        }

        /// <summary>
        /// 从Consul获取作业配置
        /// </summary>
        /// <returns></returns>
        private List<JobConfig> GetJobConfigsFromConsul()
        {
            List<JobConfig> consulJobConfigs = new List<JobConfig>();

            var jobConfigKeyPrefix = string.Format("Swift/{0}/Jobs", Name);
            var jobKeys = ConsulKV.Keys(jobConfigKeyPrefix);
            var jobConfigKeys = jobKeys?.Where(d => d.EndsWith("Config", StringComparison.Ordinal));

            if (jobConfigKeys != null && jobConfigKeys.Any())
            {
                foreach (var jobConfigKey in jobConfigKeys)
                {
                    var jobJson = ConsulKV.GetValueString(jobConfigKey);
                    var jobConfig = JsonConvert.DeserializeObject<JobConfig>(jobJson);
                    consulJobConfigs.Add(jobConfig);
                }
            }

            return consulJobConfigs;
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
        private void AsyncDoEventHandler(Action action)
        {
            Task.Factory.StartNew(action);
        }
        #endregion
    }
}
