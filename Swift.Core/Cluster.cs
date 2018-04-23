using Consul;
using Newtonsoft.Json;
using Swift.Core.Consul;
using Swift.Core.ExtensionException;
using Swift.Core.Log;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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
    /// <param name="job"></param>
    public delegate void TaskJoinEvent(JobTask task);

    /// <summary>
    /// 任务移除事件
    /// </summary>
    /// <param name="job"></param>
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
        private string name;
        private string localIP;
        private Manager manager;
        private Worker[] workers;
        private List<Member> members;
        private List<JobBase> activedJobs;
        private List<JobConfig> jobConfigs;
        private List<JobTask> activedTasks;
        private Member currentMember;
        private object refreshLocker = new object();

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="name"></param>
        public Cluster(string name, string bindingIP)
        {
            this.name = name;
            this.localIP = bindingIP;
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
        /// Manager可以订阅此事件，然后给其它成员发放作业
        /// </summary>
        public event JobConfigJoinEvent OnJobConfigJoinEventHandler;

        /// <summary>
        /// 作业配置更新事件。
        /// Manager可以订阅此事件，然后通知其它成员更新作业
        /// </summary>
        public event JobConfigRemoveEvent OnJobConfigUpdateEventHandler;

        /// <summary>
        /// 作业配置移除事件。
        /// Manager可以订阅此事件，然后通知其它成员删除作业
        /// </summary>
        public event JobConfigRemoveEvent OnJobConfigRemoveEventHandler;

        /// <summary>
        /// 任务加入事件
        /// </summary>
        public event TaskJoinEvent OnTaskJoinEventHandler;

        /// <summary>
        /// 任务移除事件
        /// </summary>
        public event TaskRemoveEvent OnTaskRemoveEventHandler;

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
        /// 获取经理
        /// </summary>
        public Manager Manager
        {
            get
            {
                return manager;
            }
        }

        /// <summary>
        /// 获取普通工人集合
        /// </summary>
        public Worker[] Workers
        {
            get
            {
                return workers;
            }
        }

        /// <summary>
        /// 获取作业列表
        /// </summary>
        public List<JobBase> Jobs
        {
            get
            {
                return activedJobs;
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

            jobConfigs = new List<JobConfig>();
            activedJobs = new List<JobBase>();
            activedTasks = new List<JobTask>();

            MonitorMember();
            MonitorJobs();
            MonitorTask();
        }

        #region 成员
        /// <summary>
        /// 成员刷新定时器
        /// </summary>
        private Timer memberRefreshTimer;

        /// <summary>
        /// 指示是否正在刷新成员
        /// </summary>
        private bool isRefreshingMembers = false;

        /// <summary>
        /// 监控成员变化
        /// </summary>
        public void MonitorMember()
        {
            RefreshMembers(null);
            memberRefreshTimer = new Timer(new TimerCallback(RefreshMembers), null, 3000, ClusterConfiguration.RefreshMemberInterval);
        }

        /// <summary>
        /// 停止监控成员变化
        /// </summary>
        public void StopMonitorMember()
        {
            memberRefreshTimer.Dispose();
        }

        /// <summary>
        /// 刷新成员
        /// </summary>
        private void RefreshMembers(object state)
        {
            if (isRefreshingMembers)
            {
                return;
            }

            isRefreshingMembers = true;

            LogWriter.Write("开始刷新内存集群成员...");

            try
            {
                UpdateMembers();
            }
            catch (Exception ex)
            {
                LogWriter.Write(string.Format("刷新内存集群成员异常:{0},{1}", ex.Message, ex.StackTrace));
                return;
            }

            currentMember = members.Where(d => d.Id == localIP).FirstOrDefault();
            manager = members.Where(d => d.Role == EnumMemberRole.Manager).Select(d => (Manager)d).FirstOrDefault();
            workers = members.Where(d => d.Role == EnumMemberRole.Worker).Select(d => (Worker)d).ToArray();

            isRefreshingMembers = false;

            LogWriter.Write("结束刷新内存集群成员。");
        }

        /// <summary>
        /// 更新成员集合
        /// </summary>
        private void UpdateMembers()
        {
            var latestMembers = GetMembersFromConsul();
            if (members == null)
            {
                members = new List<Member>();
            }

            // 更新成员状态
            for (int i = members.Count - 1; i >= 0; i--)
            {
                var member = members[i];
                var latestMember = latestMembers.Where(d => d.Id == member.Id).FirstOrDefault();
                if (latestMember != null)
                {
                    member.Status = latestMember.Status;
                    member.OnlineTime = latestMember.OnlineTime;
                    member.OfflineTime = latestMember.OfflineTime;
                }
            }

            // 获取新增的Member
            List<Member> newMemberList = new List<Member>();
            foreach (var member in latestMembers)
            {
                var oldMember = members.Where(d => d.Id == member.Id).FirstOrDefault();
                if (oldMember == null)
                {
                    newMemberList.Add(member);
                }
            }

            // 获取移除的Member
            List<Member> removeMemberList = new List<Member>();
            for (int i = members.Count - 1; i >= 0; i--)
            {
                var member = members[i];
                var newMember = latestMembers.Where(d => d.Id == member.Id).FirstOrDefault();
                if (newMember == null)
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
                    OnMemberJoinEventHandler?.Invoke(newMember);

                    LogWriter.Write(string.Format("已添加成员:{0},{1}", newMember.Id, newMember.Role.ToString()));
                }
            }

            // 移除删除的成员
            if (removeMemberList.Count > 0)
            {
                for (int i = members.Count - 1; i >= 0; i--)
                {
                    var oldMember = members[i];
                    if (removeMemberList.Where(d => d.Id == oldMember.Id).Any())
                    {
                        members.Remove(oldMember);
                        OnMemberRemoveEventHandler?.Invoke(oldMember);

                        LogWriter.Write(string.Format("已移除成员:{0},{1}", oldMember.Id, oldMember.Role.ToString()));
                    }
                }
            }
        }

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
        /// <param name="memberId"></param>
        /// <param name="role"></param>
        private Member RegisterMember(Member member)
        {
            int tryTimes = 3;
            while (tryTimes > 0)
            {
                try
                {
                    List<Member> latestMembers;
                    while (!TrySetMember(member, out latestMembers))
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

            return members.Where(d => d.Id == member.Id).FirstOrDefault();
        }

        /// <summary>
        /// 尝试设置成员
        /// </summary>
        /// <returns></returns>
        private bool TrySetMember(Member member, out List<Member> latestMemberList)
        {
            latestMemberList = null;

            List<Member> memberList = GetMembersFromConsul();

            if (member.Role == EnumMemberRole.Manager)
            {
                var currentManager = memberList.Where(d => d.Role == EnumMemberRole.Manager && d.Status == 1).FirstOrDefault();
                if (currentManager != null && currentManager.Id != member.Id)
                {
                    throw new Exception(string.Format("不能注册为Manager，已经存在:{0}", Manager.Id));
                }
            }

            Member historyMember;
            if (memberList.Where(d => d.Id == member.Id).Any())
            {
                historyMember = memberList.Where(d => d.Id == member.Id).FirstOrDefault();

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
        /// 从Consul加载集群成员
        /// </summary>
        private List<Member> GetMembersFromConsul()
        {
            List<MemberWrapper> configMemberList;
            while (!GetValidMembersFromConsul(out configMemberList))
            {
                Thread.Sleep(1000);
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
        /// 从Consul获取经过检查的有效成员
        /// </summary>
        /// <param name="configMemberList"></param>
        /// <returns></returns>
        private bool GetValidMembersFromConsul(out List<MemberWrapper> configMemberList)
        {
            var memberKey = string.Format("Swift/{0}/Members", Name);
            KVPair memberKV = ConsulKV.Get(memberKey);
            if (memberKV == null)
            {
                memberKV = ConsulKV.Create(memberKey);
            }

            configMemberList = new List<MemberWrapper>();
            if (memberKV.Value != null)
            {
                configMemberList = JsonConvert.DeserializeObject<List<MemberWrapper>>(Encoding.UTF8.GetString(memberKV.Value));
            }

            bool isNeedUpdateConfig = false;
            List<MemberWrapper> needRemoveList = new List<MemberWrapper>();
            foreach (var configMember in configMemberList)
            {
                var serviceId = string.Format("Swift-{0}-Member-{1}", Name, configMember.Id);
                var isHealth = ConsulService.CheckHealth(serviceId);
                configMember.Status = isHealth ? 1 : 0;

                if (configMember.Status == 0)
                {
                    // 还没设置离线时间，马上设置一个
                    if (!configMember.OfflineTime.HasValue)
                    {
                        isNeedUpdateConfig = true;
                        configMember.OfflineTime = DateTime.Now;
                    }
                    else
                    {
                        // 离线超过3个小时了，移除成员配置
                        if (DateTime.Now.Subtract(configMember.OfflineTime.Value).TotalHours > 3)
                        {
                            needRemoveList.Add(configMember);
                        }
                    }
                }
            }

            // 从集合中移除成员配置
            if (needRemoveList.Count > 0)
            {
                isNeedUpdateConfig = true;
                foreach (var config in needRemoveList)
                {
                    configMemberList.Remove(config);
                }
            }

            // 更新到Consul中
            if (isNeedUpdateConfig)
            {
                memberKV.Value = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(configMemberList));
                return ConsulKV.CAS(memberKV);
            }

            return true;
        }

        /// <summary>
        /// 注册到Consul
        /// </summary>
        private void RegisterToConsul()
        {
            //ConsulService.DeregisterService(localIP);
            //ConsulService.DeregisterServiceCheck("CHECK-" + localIP);

            var serviceId = string.Format("Swift-{0}-Member-{1}", Name, localIP);
            var serviceName = string.Format("Swift-{0}-Member-{1}", Name, localIP);
            ConsulService.RegisterService(serviceId, serviceName, 15);

            Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    try
                    {
                        ConsulService.PassTTL(serviceId);
                    }
                    catch (Exception ex)
                    {
                        LogWriter.Write(string.Format("Consul健康检测PassTTL异常:{0}", ex.Message + ex.StackTrace));
                        Thread.Sleep(1000);
                    }

                    Thread.Sleep(10000);
                }
            });
        }
        #endregion

        #region 任务
        /// <summary>
        /// 任务刷新定时器
        /// </summary>
        private Timer taskRefreshTimer;

        /// <summary>
        /// 指示是否正在刷新任务
        /// </summary>
        private bool isRefreshingTasks = false;

        /// <summary>
        /// 监控任务变化
        /// </summary>
        public void MonitorTask()
        {
            taskRefreshTimer = new Timer(new TimerCallback(RefreshTasks), null, 20000, 10000);
        }

        /// <summary>
        /// 停止监控任务变化
        /// </summary>
        public void StopMonitorTask()
        {
            taskRefreshTimer.Dispose();
        }

        /// <summary>
        /// 刷新任务
        /// </summary>
        public void RefreshTasks(object state)
        {
            // 如果任务刷新需要的时间超过任务刷新间隔时间，则跳过新的刷新
            if (isRefreshingTasks)
            {
                return;
            }

            isRefreshingTasks = true;

            lock (refreshLocker)
            {
                LogWriter.Write("开始刷新任务列表...");

                try
                {
                    var latestTasks = LoadTasks();

                    if (activedTasks == null)
                    {
                        activedTasks = new List<JobTask>();
                    }

                    // 获取新增的Task
                    List<JobTask> newTaskList = new List<JobTask>();
                    foreach (var task in latestTasks)
                    {
                        var oldTask = activedTasks.Where(d => d.Job.Id == task.Job.Id && d.Id == task.Id).FirstOrDefault();
                        if (oldTask == null)
                        {
                            newTaskList.Add(task);
                        }
                    }

                    // 获取移除的Task
                    List<JobTask> removeTaskList = new List<JobTask>();
                    foreach (var task in activedTasks)
                    {
                        var newTask = latestTasks.Where(d => d.Job.Id == task.Job.Id && d.Id == task.Id).FirstOrDefault();
                        if (newTask == null)
                        {
                            removeTaskList.Add(newTask);
                        }
                    }

                    // 添加新任务
                    if (newTaskList.Count > 0)
                    {
                        foreach (var newTask in newTaskList)
                        {
                            activedTasks.Add(newTask);
                            OnTaskJoinEventHandler?.Invoke(newTask);

                            LogWriter.Write(string.Format("发现集群任务:{0},{1},{2}", newTask.Job.Name, newTask.Job.Id, newTask.Id));
                        }
                    }

                    // 移除删除的任务
                    if (removeTaskList.Count > 0)
                    {
                        LogWriter.Write(string.Format("发现待删除的任务数:{0}", removeTaskList.Count));

                        var activedTasksCount = activedTasks.Count - 1;
                        for (int i = activedTasksCount; i >= 0; i--)
                        {
                            var oldTask = activedTasks[i];
                            if (removeTaskList.Where(d => d.Id == oldTask.Id).Any())
                            {
                                activedTasks.Remove(oldTask);
                                OnTaskRemoveEventHandler?.Invoke(oldTask);

                                LogWriter.Write(string.Format("移除集群任务:{0},{1},{2}", oldTask.Job.Name, oldTask.Job.Id, oldTask.Id));
                            }
                        }
                    }

                    LogWriter.Write("结束刷新任务列表。");
                }
                catch (Exception ex)
                {
                    LogWriter.Write(string.Format("刷新任务异常:{0}", ex.Message), ex);
                }
            }

            isRefreshingTasks = false;
        }

        /// <summary>
        /// 加载所有任务
        /// </summary>
        public List<JobTask> LoadTasks()
        {
            List<JobTask> taskList = new List<JobTask>();

            if (activedJobs == null || activedJobs.Count <= 0)
            {
                return taskList;
            }

            foreach (var job in activedJobs)
            {
                var tasks = LoadTasks(job);
                if (tasks != null)
                {
                    taskList.AddRange(tasks);
                }
            }

            return taskList;
        }

        /// <summary>
        /// 加载指定作业的任务
        /// </summary>
        /// <param name="job"></param>
        /// <returns></returns>
        public List<JobTask> LoadTasks(JobBase job)
        {
            List<JobTask> taskList = new List<JobTask>();

            var jobRecordKey = string.Format("Swift/{0}/Jobs/{1}/Records/{2}", Name, job.Name, job.Id);
            var jobRecordKV = ConsulKV.Get(jobRecordKey);
            if (jobRecordKV == null)
            {
                LogWriter.Write(string.Format("作业丢失:{0},{1}", job.Name, job.Id));
                return null;
            }

            var jobRecordJson = Encoding.UTF8.GetString(jobRecordKV.Value);
            var jobRecord = JobBase.Deserialize(jobRecordJson, this);
            if (jobRecord != null)
            {
                if (jobRecord.Status == EnumJobRecordStatus.Pending || jobRecord.Status == EnumJobRecordStatus.PlanMaking)
                {
                    return null;
                }

                if (jobRecord.TaskPlan != null && jobRecord.TaskPlan.Count > 0)
                {
                    foreach (var key in jobRecord.TaskPlan.Keys)
                    {
                        var taskPlan = jobRecord.TaskPlan[key];
                        if (taskPlan != null && taskPlan.Any())
                        {
                            foreach (var task in taskPlan)
                            {
                                task.Job = jobRecord;
                                taskList.Add(task);
                            }
                        }
                    }
                }
            }

            return taskList;
        }
        #endregion

        #region 作业
        /// <summary>
        /// 作业刷新定时器
        /// </summary>
        private Timer jobRefreshTimer;

        /// <summary>
        /// 指示是否正在刷新作业
        /// </summary>
        private bool isRefreshingJobs = false;

        /// <summary>
        /// 监控作业变化
        /// </summary>
        public void MonitorJobs()
        {
            jobRefreshTimer = new Timer(new TimerCallback(RefreshJob), null, 20000, ClusterConfiguration.RefreshJobInterval);
        }

        /// <summary>
        /// 停止监控作业变化
        /// </summary>
        public void StopMonitorJob()
        {
            jobRefreshTimer.Dispose();
        }

        /// <summary>
        /// 刷新作业
        /// </summary>
        /// <param name="state"></param>
        public void RefreshJob(object state)
        {
            if (isRefreshingJobs)
            {
                return;
            }

            isRefreshingJobs = true;

            lock (refreshLocker)
            {
                LogWriter.Write("开始刷新作业列表...");

                try
                {
                    // 从Consul查询作业的最新记录
                    if (jobConfigs != null && jobConfigs.Count > 0)
                    {
                        for (int i = jobConfigs.Count - 1; i >= 0; i--)
                        {
                            var jobConfig = jobConfigs[i];

                            // 如果有新的作业记录了，旧的作业记录将删除
                            var notCurrentJobRecord = activedJobs.Where(d => d.Name == jobConfig.Name && d.Id != jobConfig.LastRecordId);
                            if (notCurrentJobRecord.Any())
                            {
                                foreach (var oldJobRecord in notCurrentJobRecord)
                                {
                                    activedJobs.Remove(oldJobRecord);
                                    OnJobRemoveEventHandler?.Invoke(oldJobRecord);
                                    LogWriter.Write(string.Format("已从内存移除作业记录:{0},{1}", jobConfig.Name, oldJobRecord.Id));
                                }
                            }

                            LogWriter.Write(string.Format("作业LastRecordId:{0},{1}", jobConfig.Name, jobConfig.LastRecordId));

                            if (!string.IsNullOrWhiteSpace(jobConfig.LastRecordId))
                            {
                                var jobRecordKey = string.Format("Swift/{0}/Jobs/{1}/Records/{2}", Name, jobConfig.Name, jobConfig.LastRecordId);
                                var jobRecordKV = ConsulKV.Get(jobRecordKey);
                                if (jobRecordKV == null)
                                {
                                    LogWriter.Write(string.Format("作业记录不存在或者已经被删除:{0},{1}", jobConfig.Name, jobConfig.LastRecordId));

                                    // 如果还在内存中，则从内存移除此记录
                                    var memoryJob = activedJobs.Where(d => d.Id == jobConfig.LastRecordId).FirstOrDefault();
                                    if (memoryJob != null)
                                    {
                                        activedJobs.Remove(memoryJob);
                                        OnJobRemoveEventHandler?.Invoke(memoryJob);
                                        LogWriter.Write(string.Format("已从内存移除作业记录:{0},{1}", jobConfig.Name, memoryJob.Id));
                                    }

                                    continue;
                                }

                                if (!activedJobs.Where(d => d.Id == jobConfig.LastRecordId).Any())
                                {
                                    // 内存中无此作业
                                    var jobRecord = JobBase.Deserialize(Encoding.UTF8.GetString(jobRecordKV.Value), this);
                                    jobRecord.ModifyIndex = jobRecordKV.ModifyIndex;
                                    activedJobs.Add(jobRecord);
                                    OnJobJoinEventHandler?.Invoke(jobRecord);
                                    LogWriter.Write(string.Format("发现新作业记录:{0},{1}", jobConfig.Name, jobRecord.Id));
                                }
                                else
                                {
                                    // 作业有更新
                                    var exsitJob = activedJobs.Where(d => d.Id == jobConfig.LastRecordId).FirstOrDefault();
                                    if (exsitJob != null && exsitJob.ModifyIndex != jobRecordKV.ModifyIndex)
                                    {
                                        var jobRecord = JobBase.Deserialize(Encoding.UTF8.GetString(jobRecordKV.Value), this);
                                        jobRecord.ModifyIndex = jobRecordKV.ModifyIndex;
                                        exsitJob.UpdateFrom(jobRecord);

                                        LogWriter.Write(string.Format("作业记录有更新:{0}", jobConfig.LastRecordId));
                                    }
                                }
                            }
                        }
                    }

                    LogWriter.Write("结束刷新作业列表。");
                }
                catch (Exception ex)
                {
                    LogWriter.Write(string.Format("刷新作业记录异常:{0}", ex.Message));
                }
            }

            isRefreshingJobs = false;
        }
        #endregion

        #region 作业配置
        /// <summary>
        /// 作业配置刷新定时器
        /// </summary>
        private Timer jobConfigRefreshTimer;

        /// <summary>
        /// Consul作业配置刷新定时器
        /// </summary>
        private Timer jobConfigConsulRefreshTimer;

        /// <summary>
        /// 作业创建定时器
        /// </summary>
        private Timer jobCreateTimer;

        /// <summary>
        /// 指示是否正在刷新作业配置
        /// </summary>
        private bool isRefreshingJobConfigs = false;

        /// <summary>
        /// 监控作业配置变化 Worker将调用此方法，以更新集群作业配置
        /// </summary>
        public void MonitorJobConfigsFromConsul()
        {
            jobConfigConsulRefreshTimer = new Timer(new TimerCallback(RefreshJobConfigsFromConsul), null, 5000, ClusterConfiguration.RefreshJobConfigInterval);
        }

        /// <summary>
        /// 监控作业配置变化
        /// Manager将调用此方法，以更新集群作业配置
        /// </summary>
        public void MonitorJobConfigsFromDisk()
        {
            jobConfigRefreshTimer = new Timer(new TimerCallback(RefreshJobConfigsFromDisk), null, 5000, ClusterConfiguration.RefreshJobConfigInterval);
            jobCreateTimer = new Timer(new TimerCallback(TimingCreateJob), null, 10000, ClusterConfiguration.JobSpaceCreateInterval);
        }

        /// <summary>
        /// 停止监控作业配置变化 Manager将调用此方法，以停止更新集群作业配置
        /// </summary>
        public void StopMonitorJobConfigsFromDisk()
        {
            if (jobConfigRefreshTimer != null)
            {
                jobConfigRefreshTimer.Dispose();
            }

            if (jobCreateTimer != null)
            {
                jobCreateTimer.Dispose();
            }
        }

        /// <summary>
        /// 停止监控作业配置变化 Worker将调用此方法，以停止更新集群作业配置
        /// </summary>
        public void StopMonitorJobConfigsFromConsul()
        {
            if (jobConfigConsulRefreshTimer != null)
            {
                jobConfigConsulRefreshTimer.Dispose();
            }
        }

        /// <summary>
        /// 定时创建作业
        /// </summary>
        /// <param name="state"></param>
        private void TimingCreateJob(object state)
        {
            lock (refreshLocker)
            {
                if (jobConfigs != null && jobConfigs.Count > 0)
                {
                    LogWriter.Write(string.Format("开始定时创建作业检查..."));

                    foreach (var jobConfig in jobConfigs)
                    {
                        if (!string.IsNullOrWhiteSpace(jobConfig.LastRecordId))
                        {
                            var lastJobKey = string.Format("Swift/{0}/Jobs/{1}/Records/{2}", Name, jobConfig.Name, jobConfig.LastRecordId);
                            var lastJobKV = ConsulKV.Get(lastJobKey);
                            if (lastJobKV != null)
                            {
                                var lastJob = JsonConvert.DeserializeObject<JobWrapper>(Encoding.UTF8.GetString(lastJobKV.Value));
                                if (lastJob.Status != EnumJobRecordStatus.TaskMerged)
                                {
                                    LogWriter.Write(string.Format("上一次作业记录未完成:{0},{1}", jobConfig.Name, jobConfig.LastRecordId));
                                    continue;
                                }
                            }
                        }

                        if (jobConfig.RunTimes != null && jobConfig.RunTimes.Length > 0)
                        {
                            foreach (var timePlan in jobConfig.RunTimes)
                            {
                                if (timePlan.CheckIsTime())
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
                                        jobConfig.LastRecordId = job.Id;
                                        jobConfig.LastRecordStartTime = DateTime.Now;
                                        jobConfig.ModifyIndex = jobConfigKV.ModifyIndex;
                                        var jobConfigJson = JsonConvert.SerializeObject(jobConfig);
                                        jobConfigKV.Value = Encoding.UTF8.GetBytes(jobConfigJson);
                                        ConsulKV.CAS(jobConfigKV);

                                        // 更新本地作业配置
                                        var jobConfigLocalPath = Path.Combine(Environment.CurrentDirectory, "Jobs", jobConfig.Name, "config", "job.json");
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
        }

        /// <summary>
        /// 通过磁盘刷新作业配置
        /// </summary>
        private void RefreshJobConfigsFromDisk(object state)
        {
            if (isRefreshingJobConfigs)
            {
                return;
            }

            isRefreshingJobConfigs = true;

            lock (refreshLocker)
            {
                try
                {
                    var latestJobConfigs = LoadJobConfigsFromDisk();
                    LogWriter.Write(string.Format("当前作业配置数量:{0}", latestJobConfigs.Count));

                    if (jobConfigs == null)
                    {
                        jobConfigs = new List<JobConfig>();
                    }

                    // 获取新增的JobConfig
                    List<JobConfig> newJobConfigList = new List<JobConfig>();
                    foreach (var jobConfig in latestJobConfigs)
                    {
                        var oldJobConfig = jobConfigs.Where(d => d.Name == jobConfig.Name).FirstOrDefault();
                        if (oldJobConfig == null)
                        {
                            newJobConfigList.Add(jobConfig);
                        }
                    }

                    // 获取移除的JobConfig
                    List<JobConfig> removeJobConfigList = new List<JobConfig>();
                    foreach (var jobConfig in jobConfigs)
                    {
                        var newJobConfig = latestJobConfigs.Where(d => d.Name == jobConfig.Name).FirstOrDefault();
                        if (newJobConfig == null)
                        {
                            removeJobConfigList.Add(newJobConfig);
                        }
                    }

                    // 获取更新的JobConfig
                    List<JobConfig> updateJobConfigList = new List<JobConfig>();
                    for (int i = jobConfigs.Count - 1; i >= 0; i--)
                    {
                        var jobConfig = jobConfigs[i];
                        var latestJobConfig = latestJobConfigs.Where(d => d.Name == jobConfig.Name).FirstOrDefault();
                        if (latestJobConfig != null && latestJobConfig.Version != jobConfig.Version)
                        {
                            updateJobConfigList.Add(latestJobConfig);
                        }
                    }

                    // 添加新JobConfig
                    if (newJobConfigList.Count > 0)
                    {
                        for (int i = newJobConfigList.Count - 1; i >= 0; i--)
                        {
                            // 内存中添加
                            var newJobConfig = newJobConfigList[i];
                            LogWriter.Write("开始添加作业配置[" + newJobConfig.Name + "]");

                            jobConfigs.Add(newJobConfig);
                            LogWriter.Write("已添加作业配置[" + newJobConfig.Name + "]到内存");

                            // Consul中添加
                            var result = TryAddJobConfig(newJobConfig);
                            LogWriter.Write("添加作业配置[" + newJobConfig.Name + "]到Consul结果:" + result.ToString());

                            if (result)
                            {
                                OnJobConfigJoinEventHandler?.Invoke(newJobConfig);
                            }
                        }
                    }

                    // 更新JobConfig
                    if (updateJobConfigList.Count > 0)
                    {
                        for (int i = updateJobConfigList.Count - 1; i >= 0; i--)
                        {
                            var updateJobConfig = jobConfigs.Where(d => d.Name == updateJobConfigList[i].Name).FirstOrDefault();
                            LogWriter.Write("开始更新作业配置[" + updateJobConfig.Name + "]");

                            // 内存中更新
                            updateJobConfig = updateJobConfigList[i];
                            LogWriter.Write("已更新内存中作业配置[" + updateJobConfig.Name + "]");

                            // Consul中更新
                            UpdateJobConfig(updateJobConfig);
                            LogWriter.Write("已更新Consul中作业配置[" + updateJobConfig.Name + "]");

                            OnJobConfigUpdateEventHandler?.Invoke(updateJobConfig);
                        }
                    }

                    // 移除删除的JobConfig
                    if (removeJobConfigList.Count > 0)
                    {
                        for (int i = removeJobConfigList.Count - 1; i >= 0; i--)
                        {
                            var removeJobConfig = jobConfigs.Where(d => d.Name == removeJobConfigList[i].Name).FirstOrDefault();
                            LogWriter.Write("开始移除作业配置[" + removeJobConfig.Name + "]");

                            // 内存中移除
                            var mRemoveResult = jobConfigs.Remove(removeJobConfig);
                            LogWriter.Write("内存中移除作业配置[" + removeJobConfig.Name + "]结果:" + mRemoveResult.ToString());

                            // Consul中移除
                            var configRemoveResult = RemoveJobConfig(removeJobConfig);
                            LogWriter.Write("Consul中移除作业配置[" + removeJobConfig.Name + "]结果:" + configRemoveResult.ToString());

                            OnJobConfigRemoveEventHandler?.Invoke(removeJobConfig);
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogWriter.Write(string.Format("刷新作业配置异常:{0}", ex.Message));
                }
            }

            isRefreshingJobConfigs = false;
        }

        /// <summary>
        /// 通过Consul刷新作业配置
        /// </summary>
        /// <param name="state"></param>
        private void RefreshJobConfigsFromConsul(object state)
        {
            if (isRefreshingJobConfigs)
            {
                return;
            }

            isRefreshingJobConfigs = true;

            lock (refreshLocker)
            {
                try
                {
                    var latestJobConfigs = LoadJobConfigsFromConsul();
                    LogWriter.Write(string.Format("发现作业配置数量:{0}", latestJobConfigs.Count));

                    if (jobConfigs == null)
                    {
                        jobConfigs = new List<JobConfig>();
                    }

                    // 获取新增的JobConfig
                    List<JobConfig> newJobConfigList = new List<JobConfig>();
                    foreach (var jobConfig in latestJobConfigs)
                    {
                        var oldJobConfig = jobConfigs.Where(d => d.Name == jobConfig.Name).FirstOrDefault();
                        if (oldJobConfig == null)
                        {
                            newJobConfigList.Add(jobConfig);
                        }
                    }

                    // 获取移除的JobConfig
                    List<string> removeJobConfigList = new List<string>();
                    foreach (var jobConfig in jobConfigs)
                    {
                        var newJobConfig = latestJobConfigs.Where(d => d.Name == jobConfig.Name).FirstOrDefault();
                        if (newJobConfig == null)
                        {
                            removeJobConfigList.Add(newJobConfig.Name);
                        }
                    }

                    // 更新JobConfig
                    for (int i = jobConfigs.Count - 1; i >= 0; i--)
                    {
                        var jobConfig = jobConfigs[i];
                        var latestJobConfig = latestJobConfigs.Where(d => d.Name == jobConfig.Name).FirstOrDefault();
                        if (latestJobConfig != null)
                        {
                            if (latestJobConfig.ModifyIndex != jobConfig.ModifyIndex)
                            {
                                jobConfigs[i] = latestJobConfig;
                            }

                            if (latestJobConfig.Version != jobConfig.Version)
                            {
                                jobConfigs[i] = latestJobConfig;
                                OnJobConfigUpdateEventHandler?.Invoke(latestJobConfig);
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

                            OnJobConfigJoinEventHandler?.Invoke(newJobConfig);
                        }
                    }

                    // 移除删除的JobConfig
                    if (removeJobConfigList.Count > 0)
                    {
                        for (int i = jobConfigs.Count - 1; i >= 0; i--)
                        {
                            if (removeJobConfigList.Where(d => d == jobConfigs[i].Name).Any())
                            {
                                var removeJobConfig = jobConfigs[i];

                                var mRemoveResult = jobConfigs.Remove(removeJobConfig);
                                LogWriter.Write("内存中移除作业配置[" + removeJobConfig.Name + "]结果:" + mRemoveResult.ToString());

                                OnJobConfigRemoveEventHandler?.Invoke(removeJobConfig);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogWriter.Write(string.Format("刷新作业配置异常:{0},{1}", ex.Message, ex.StackTrace));
                }
            }

            isRefreshingJobConfigs = false;
        }

        /// <summary>
        /// 尝试添加作业配置，如果作业配置不存在，则返回true，否则返回false
        /// </summary>
        /// <param name="jobConfig"></param>
        /// <returns></returns>
        private bool TryAddJobConfig(JobConfig jobConfig)
        {
            // TODO:Consul中可能已经存在了,这时候走更新UpdateJobConfig

            var jobConfigKey = string.Format("Swift/{0}/Jobs/{1}/Config", Name, jobConfig.Name);
            var configKV = ConsulKV.Get(jobConfigKey);
            if (configKV == null)
            {
                configKV = ConsulKV.Create(jobConfigKey);
            }

            configKV.Value = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(jobConfig));
            var result = ConsulKV.CAS(configKV);
            return result;
        }

        /// <summary>
        /// 更新作业配置到Conusl
        /// </summary>
        /// <param name="jobConfig"></param>
        /// <returns></returns>
        private bool UpdateJobConfig(JobConfig jobConfig)
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
        private bool RemoveJobConfig(JobConfig jobConfig)
        {
            var jobConfigKey = string.Format("Swift/{0}/Jobs/{1}", Name, jobConfig.Name);
            return ConsulKV.DeleteTree(jobConfigKey);
        }

        /// <summary>
        /// 从磁盘加载作业配置
        /// </summary>
        /// <returns></returns>
        private List<JobConfig> LoadJobConfigsFromDisk()
        {
            List<JobConfig> latestJobConfigs = new List<JobConfig>();

            var jobRootPath = Path.Combine(Environment.CurrentDirectory, "Jobs");
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
            // 当前作业包更新时间
            var pkgUpdateTime = File.GetLastWriteTime(pkgPath);

            // 当前作业包版本
            var pkgVersion = pkgUpdateTime.ToString("yyyyMMddHHmmss");

            // 作业根目录
            var jobRootPath = Path.Combine(Environment.CurrentDirectory, "Jobs");

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

            // 如果是旧的作业包，更新作业包配置
            if (!string.IsNullOrWhiteSpace(jobConfig.Version))
            {
                ExtractJobConfigFile(pkgPath, jobConfigDirectory, true);
                var latestJobConfig = new JobConfig(jobConfigPath);
                latestJobConfig.LastRecordId = jobConfig.LastRecordId;
                latestJobConfig.LastRecordStartTime = jobConfig.LastRecordStartTime;
                latestJobConfig.ModifyIndex = jobConfig.ModifyIndex;
                latestJobConfig.Version = pkgVersion;
                UpdateJobConfigFile(jobConfigPath, latestJobConfig);

                jobConfig = latestJobConfig;
            }

            return jobConfig;
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
        /// <param name="pkgPath"></param>
        /// <param name="configDirectory"></param>
        /// <param name="isCover">是否覆盖</param>
        private static void ExtractJobConfigFile(string pkgPath, string configDirectory, bool isCover = false)
        {
            if (isCover || !Directory.Exists(configDirectory))
            {
                Directory.CreateDirectory(configDirectory);
                var filePath = Path.Combine(configDirectory, "job.json");

                if (isCover || !File.Exists(filePath))
                {
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
        }

        /// <summary>
        /// 从Consul加载作业配置
        /// </summary>
        /// <returns></returns>
        private List<JobConfig> LoadJobConfigsFromConsul()
        {
            List<JobConfig> newJobConfigs = new List<JobConfig>();

            var jobConfigKeyPrefix = string.Format("Swift/{0}/Jobs", Name);
            var jobKeys = ConsulKV.Keys(jobConfigKeyPrefix);
            var jobConfigKeys = jobKeys?.Where(d => d.EndsWith("Config"));

            if (jobConfigKeys != null && jobConfigKeys.Any())
            {
                foreach (var jobConfigKey in jobConfigKeys)
                {
                    var jobJson = ConsulKV.GetValueString(jobConfigKey);
                    var jobConfig = JsonConvert.DeserializeObject<JobConfig>(jobJson);
                    newJobConfigs.Add(jobConfig);
                }
            }

            return newJobConfigs;
        }
        #endregion

        #region 其它
        /// <summary>
        /// 获取本机IP
        /// </summary>
        /// <returns></returns>
        private string GetLocalIP()
        {
            List<string> urlList = new List<string>();
            string name = Dns.GetHostName();
            IPAddress[] ipadrlist = Dns.GetHostAddresses(name);

            foreach (IPAddress ipa in ipadrlist)
            {
                var ip = ipa.ToString();
                if (!ipa.IsIPv6LinkLocal && !ipa.IsIPv6Multicast && !ipa.IsIPv6SiteLocal && !ipa.IsIPv6Teredo && !ip.StartsWith("169"))
                {
                    urlList.Add(ip);
                }
            }

            if (urlList.Contains("127.0.0.1"))
            {
                urlList.Remove("127.0.0.1");

            }

            if (urlList.Contains("localhost"))
            {
                urlList.Remove("localhost");
            }

            return urlList.FirstOrDefault();
        }
        #endregion
    }
}
