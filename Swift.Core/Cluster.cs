using Newtonsoft.Json;
using Swift.Core.Consul;
using Swift.Core.ExtensionException;
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
        /// 监控成员变化
        /// </summary>
        public void MonitorMember()
        {
            RefreshMembers(null);
            memberRefreshTimer = new Timer(new TimerCallback(RefreshMembers), null, 3000, 3000);
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
            WriteLog("开始刷新内存集群成员...");

            try
            {
                UpdateMemoryMembers();
            }
            catch (Exception ex)
            {
                WriteLog(string.Format("刷新内存集群成员异常:{0},{1}", ex.Message, ex.StackTrace));
                return;
            }

            currentMember = members.Where(d => d.Id == localIP).FirstOrDefault();
            manager = (Manager)members.Where(d => d.Role == EnumMemberRole.Manager).FirstOrDefault();
            workers = members.Where(d => d.Role == EnumMemberRole.Worker).Select(d => (Worker)d).ToArray();

            WriteLog("结束刷新内存集群成员。");
        }

        /// <summary>
        /// 更新内存中的成员
        /// </summary>
        private void UpdateMemoryMembers()
        {
            var latestMembers = GetMembersFromConsul();
            if (members == null)
            {
                members = new List<Member>();
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
            foreach (var member in members)
            {
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

                    WriteLog(string.Format("已添加成员:{0},{1}", newMember.Id, newMember.Role.ToString()));
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

                        WriteLog(string.Format("已移除成员:{0},{1}", oldMember.Id, oldMember.Role.ToString()));
                    }
                }
            }
        }

        /// <summary>
        /// 注册成员
        /// </summary>
        /// <param name="memberId"></param>
        /// <param name="role"></param>
        public Member RegisterMember(string memberId, EnumMemberRole role)
        {
            // 创建当前成员
            var currentMember = members.Where(d => d.Id == memberId).FirstOrDefault();
            if (currentMember != null)
            {
                WriteLog("当前成员已加入过集群。");
            }

            try
            {
                // 注册经理
                if (role == EnumMemberRole.Manager)
                {
                    RegisterManager(memberId, currentMember);
                }

                // 注册工人
                if (role == EnumMemberRole.Worker)
                {
                    RegisterWorker(memberId, currentMember);
                }
            }
            catch (Exception ex)
            {
                WriteLog(string.Format("出现异常：{0}", ex.Message));
            }

            // 刷新到最新配置
            RefreshMembers(null);

            return members.Where(d => d.Id == memberId).FirstOrDefault();
        }

        /// <summary>
        /// 注册为Manager
        /// </summary>
        private void RegisterManager(string memberId, Member currentMember)
        {
            // 经理已经存在
            if (Manager != null)
            {
                // 不是当前成员
                if (Manager.Id != memberId)
                {
                    throw new Exception(string.Format("注册为Manager错误，已经存在:{0}", Manager.Id));
                }
                else
                {
                    WriteLog("当前成员已经是Manager。");
                }
            }
            else
            {
                if (currentMember != null)
                {
                    WriteLog(string.Format("当前成员的历史角色为:{0}，将尝试移除", currentMember.Role.ToString()));
                    while (!TryRemoveMember(memberId, currentMember.Role))
                    {
                        Thread.Sleep(100);
                    }
                    WriteLog(string.Format("成员的历史角色已经移除。"));
                }

                var setResult = TrySetManager(memberId, EnumMemberRole.Manager);

                if (!setResult)
                {
                    WriteLog(string.Format("当前成员注册失败，角色被强占。"));
                    return;
                }

                WriteLog(string.Format("当前成员注册为Manager成功。"));
            }
        }

        /// <summary>
        /// 注册为Worker
        /// </summary>
        private void RegisterWorker(string memberId, Member currentMember)
        {
            if (currentMember != null && currentMember.Role != EnumMemberRole.Worker)
            {
                WriteLog(string.Format("当前成员的历史角色为:{0}，将尝试移除", currentMember.Role.ToString()));
                while (!TryRemoveMember(memberId, currentMember.Role))
                {
                    Thread.Sleep(100);
                }
                WriteLog(string.Format("成员的历史角色已经移除", currentMember.Role.ToString()));
            }

            // 可尝试多次，普通工人越多越好
            while (!TrySetWorker(memberId))
            {
                Thread.Sleep(100);
            }

            WriteLog(string.Format("当前成员注册为Worker成功。"));
        }

        /// <summary>
        /// 尝试设置成员为经理：只能设置一次
        /// </summary>
        /// <param name="memberId"></param>
        /// <param name="role"></param>
        /// <returns></returns>
        private bool TrySetManager(string memberId, EnumMemberRole role)
        {
            var memberKey = string.Format("Swift/{0}/Members/{1}", Name, role.ToString());
            var memberKV = ConsulKV.Get(memberKey);
            if (memberKV == null)
            {
                memberKV = ConsulKV.Create(memberKey);
            }
            var memberKVV = memberKV.Value != null ? Encoding.UTF8.GetString(memberKV.Value) : string.Empty;

            if (memberId != memberKVV)
            {
                memberKV.Value = Encoding.UTF8.GetBytes(memberId);
                return ConsulKV.CAS(memberKV);
            }

            return true;
        }

        /// <summary>
        /// 尝试设置成员为普通工人：可连续多次尝试设置
        /// </summary>
        /// <param name="memberId"></param>
        /// <returns></returns>
        private bool TrySetWorker(string memberId)
        {
            var memberKey = string.Format("Swift/{0}/Members/{1}", Name, "Worker");
            var memberKV = ConsulKV.Get(memberKey);
            if (memberKV == null)
            {
                memberKV = ConsulKV.Create(memberKey);
            }

            var memberKVV = memberKV.Value != null ? Encoding.UTF8.GetString(memberKV.Value) : string.Empty;

            var workers = memberKVV.Split(',').ToList();
            if (!workers.Contains(memberId))
            {
                workers.Add(memberId);
                var workersStr = string.Join(",", workers.Where(d => !string.IsNullOrWhiteSpace(d)).ToArray());
                memberKV.Value = Encoding.UTF8.GetBytes(workersStr);

                return ConsulKV.CAS(memberKV);
            }

            return true;
        }

        /// <summary>
        /// 尝试移除成员，如果数据已经变更，将移除失败。
        /// 对于Manager和ViceManager只有为当前成员时才移除，对于Worker只有包含当前成员时才移除,同时使用Check ModifyIndex And Set机制，不会造成脏数据问题。
        /// </summary>
        /// <param name="memberId"></param>
        /// <param name="role"></param>
        private bool TryRemoveMember(string memberId, EnumMemberRole role)
        {
            var memberKV = ConsulKV.Get(string.Format("Swift/{0}/Members/{1}", Name, role.ToString()));
            var memberKVV = Encoding.UTF8.GetString(memberKV.Value);

            if (role == EnumMemberRole.Manager)
            {
                if (memberId == memberKVV)
                {
                    memberKV.Value = new byte[0];

                    return ConsulKV.CAS(memberKV);
                }
            }

            if (role == EnumMemberRole.Worker)
            {
                var workers = memberKVV.Split(',');
                if (workers.Contains(memberId))
                {
                    var workersStr = string.Join(",", workers.Where(d => d != memberId).ToArray());
                    memberKV.Value = Encoding.UTF8.GetBytes(workersStr);

                    return ConsulKV.CAS(memberKV);
                }
            }

            return true;
        }

        /// <summary>
        /// 从Consul加载集群成员
        /// </summary>
        private Member[] GetMembersFromConsul()
        {
            List<Member> currentMembers = new List<Member>();
            string managerStr = ConsulKV.GetValueString(string.Format("Swift/{0}/Members/Manager", Name));
            var workersStr = ConsulKV.GetValueString(string.Format("Swift/{0}/Members/Worker", Name));

            // 添加经理
            if (!string.IsNullOrWhiteSpace(managerStr))
            {
                var manager = new Manager()
                {
                    Id = managerStr,
                    Role = EnumMemberRole.Manager,
                    Status = 0,
                    Cluster = this,
                };
                currentMembers.Add(manager);
            }

            // 添加工人
            if (!string.IsNullOrWhiteSpace(workersStr))
            {
                var workersStrArray = workersStr.Split(',');
                foreach (var workerStr in workersStrArray)
                {
                    if (!string.IsNullOrWhiteSpace(workersStr))
                    {
                        var worker = new Worker()
                        {
                            Id = workerStr,
                            Role = EnumMemberRole.Worker,
                            Status = 0,
                            Cluster = this,
                        };

                        currentMembers.Add(worker);
                    }
                }
            }

            // 通过服务发现检查集群成员的健康状态
            foreach (var member in currentMembers)
            {
                var isHealth = ConsulService.CheckHealth(member.Id);
                if (isHealth)
                {
                    member.Status = 1;
                }
            }

            return currentMembers.ToArray();
        }
        #endregion

        #region 任务
        /// <summary>
        /// 任务刷新定时器
        /// </summary>
        private Timer taskRefreshTimer;

        /// <summary>
        /// 监控任务变化
        /// </summary>
        public void MonitorTask()
        {
            taskRefreshTimer = new Timer(new TimerCallback(RefreshTasks), null, 40000, 10000);
        }

        /// <summary>
        /// 刷新任务
        /// </summary>
        public void RefreshTasks(object state)
        {
            lock (refreshLocker)
            {
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

                            WriteLog(string.Format("已添加新任务:{0},{1},{2}", newTask.Job.Name, newTask.Job.Id, newTask.Id));
                        }
                    }

                    // 移除删除的任务
                    if (removeTaskList.Count > 0)
                    {
                        for (int i = activedTasks.Count - 1; i >= 0; i--)
                        {
                            var oldTask = activedTasks[i];
                            if (removeTaskList.Where(d => d.Id == oldTask.Id).Any())
                            {
                                activedTasks.Remove(oldTask);
                                OnTaskRemoveEventHandler?.Invoke(oldTask);

                                WriteLog(string.Format("已移除任务:{0},{1},{2}", oldTask.Job.Name, oldTask.Job.Id, oldTask.Id));
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    WriteLog(string.Format("刷新任务异常:{0}", ex.StackTrace));
                }

            }
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
                WriteLog(string.Format("作业丢失:{0},{1}", job.Name, job.Id));
                return null;
            }

            var jobRecordJson = Encoding.UTF8.GetString(jobRecordKV.Value);
            var jobRecord = JobBase.Deserialize(jobRecordJson, this);
            if (jobRecord != null)
            {
                if (jobRecord.Status == EnumJobRecordStatus.Pending || jobRecord.Status == EnumJobRecordStatus.PlanMaking)
                {
                    //WriteLog(string.Format("作业任务尚未发放:{0},{1}", job.Name, job.Id));
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
        /// 监控作业变化
        /// </summary>
        public void MonitorJobs()
        {
            jobRefreshTimer = new Timer(new TimerCallback(RefreshJob), null, 30000, 10000);
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
            lock (refreshLocker)
            {
                WriteLog("开始刷新作业列表...");

                try
                {
                    // 从Consul查询作业的最新记录
                    if (jobConfigs != null && jobConfigs.Count > 0)
                    {
                        foreach (var jobConfig in jobConfigs)
                        {
                            // 如果有新的作业记录了，旧的作业记录将删除
                            var notCurrentJobRecord = activedJobs.Where(d => d.Name == jobConfig.Name && d.Id != jobConfig.LastRecordId);
                            if (notCurrentJobRecord.Any())
                            {
                                foreach (var oldJobRecord in notCurrentJobRecord)
                                {
                                    activedJobs.Remove(oldJobRecord);
                                    OnJobRemoveEventHandler?.Invoke(oldJobRecord);
                                    WriteLog(string.Format("已从内存移除作业记录:{0},{1}", jobConfig.Name, oldJobRecord.Id));
                                }
                            }

                            WriteLog(string.Format("作业LastRecordId:{0},{1}", jobConfig.Name, jobConfig.LastRecordId));

                            if (!string.IsNullOrWhiteSpace(jobConfig.LastRecordId))
                            {
                                var jobRecordKey = string.Format("Swift/{0}/Jobs/{1}/Records/{2}", Name, jobConfig.Name, jobConfig.LastRecordId);
                                var jobRecordKV = ConsulKV.Get(jobRecordKey);
                                if (jobRecordKV == null)
                                {
                                    WriteLog(string.Format("作业记录不存在或者已经被删除:{0},{1}", jobConfig.Name, jobConfig.LastRecordId));

                                    // 如果还在内存中，则从内存移除此记录
                                    var memoryJob = activedJobs.Where(d => d.Id == jobConfig.LastRecordId).FirstOrDefault();
                                    if (memoryJob != null)
                                    {
                                        activedJobs.Remove(memoryJob);
                                        OnJobRemoveEventHandler?.Invoke(memoryJob);
                                        WriteLog(string.Format("已从内存移除作业记录:{0},{1}", jobConfig.Name, memoryJob.Id));
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
                                    WriteLog(string.Format("发现新作业记录:{0},{1}", jobConfig.Name, jobRecord.Id));
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

                                        WriteLog(string.Format("作业记录有更新:{0}", jobConfig.LastRecordId));
                                    }
                                }
                            }
                        }
                    }

                    WriteLog("结束刷新作业列表。");
                }
                catch (Exception ex)
                {
                    WriteLog(string.Format("刷新作业记录异常:{0}", ex.Message));
                }
            }
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
        /// 监控作业配置变化 Worker将调用此方法，以更新集群作业配置
        /// </summary>
        public void MonitorJobConfigsFromConsul()
        {
            jobConfigConsulRefreshTimer = new Timer(new TimerCallback(RefreshJobConfigsFromConsul), null, 5000, 30000);
        }

        /// <summary>
        /// 监控作业配置变化
        /// Manager将调用此方法，以更新集群作业配置
        /// </summary>
        public void MonitorJobConfigsFromDisk()
        {
            jobConfigRefreshTimer = new Timer(new TimerCallback(RefreshJobConfigsFromDisk), null, 5000, 30000);
            jobCreateTimer = new Timer(new TimerCallback(TimingCreateJob), null, 10000, 30000);
        }

        /// <summary>
        /// 停止监控作业配置变化 Manager将调用此方法，以停止更新集群作业配置
        /// </summary>
        public void StopMonitorJobConfigsFromDisk()
        {
            jobConfigRefreshTimer.Dispose();
            jobCreateTimer.Dispose();
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
                    WriteLog(string.Format("开始定时创建作业检查..."));

                    var nowHourAndMinute = DateTime.Now.ToString("HH:mm");
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
                                    WriteLog(string.Format("上一次作业记录未完成:{0},{1}", jobConfig.Name, jobConfig.LastRecordId));
                                    continue;
                                }
                            }
                        }

                        if (jobConfig.RunTimePlan != null && jobConfig.RunTimePlan.Length > 0)
                        {
                            foreach (var timePlan in jobConfig.RunTimePlan)
                            {
                                if (!string.IsNullOrWhiteSpace(timePlan) && nowHourAndMinute == timePlan)
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
                                        WriteLog(string.Format("已创建作业:{0},{1}", jobConfig.Name, job.Id));



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

                                        WriteLog(string.Format("已更新作业配置:{0}", jobConfig.Name));
                                    }
                                }
                            }
                        }
                    }

                    WriteLog(string.Format("结束定时创建作业检查。"));
                }
            }
        }

        /// <summary>
        /// 通过磁盘刷新作业配置
        /// </summary>
        private void RefreshJobConfigsFromDisk(object state)
        {
            lock (refreshLocker)
            {
                try
                {
                    var latestJobConfigs = LoadJobConfigsFromDisk();
                    WriteLog(string.Format("发现作业配置数量:{0}", latestJobConfigs.Count));

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

                    // 更新配置
                    for (int i = jobConfigs.Count - 1; i >= 0; i--)
                    {
                        var jobConfig = jobConfigs[i];
                        var latestJobConfig = latestJobConfigs.Where(d => d.Name == jobConfig.Name).FirstOrDefault();
                        if (latestJobConfig != null && latestJobConfig.ModifyIndex != jobConfig.ModifyIndex)
                        {
                            jobConfig = latestJobConfig;
                        }
                    }

                    // 添加新JobConfig
                    if (newJobConfigList.Count > 0)
                    {
                        foreach (var jobConfig in newJobConfigList)
                        {
                            WriteLog("开始保存作业配置：" + jobConfig.Name);
                            jobConfigs.Add(jobConfig);
                            var result = TryAddJobConfig(jobConfig);
                            WriteLog("保存作业配置结果：" + result.ToString());

                            if (result)
                            {
                                OnJobConfigJoinEventHandler?.Invoke(jobConfig);
                            }
                        }
                    }

                    // 移除删除的JobConfig
                    if (removeJobConfigList.Count > 0)
                    {
                        for (int i = jobConfigs.Count - 1; i >= 0; i--)
                        {
                            if (removeJobConfigList.Where(d => d.Name == jobConfigs[i].Name).Any())
                            {
                                var removeJobConfig = jobConfigs[i];
                                WriteLog("开始移除作业配置：" + removeJobConfig.Name.ToString());

                                // 从内存中移除
                                var mRemoveResult = jobConfigs.Remove(removeJobConfig);
                                WriteLog("内存中移除作业配置结果：" + mRemoveResult.ToString());

                                // TODO:可以仅作删除标记
                                // 从配置中移除
                                var configRemoveResult = RemoveJobConfig(removeJobConfig);
                                WriteLog("配置中移除作业配置结果：" + configRemoveResult.ToString());

                                OnJobConfigRemoveEventHandler?.Invoke(removeJobConfig);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    WriteLog(string.Format("刷新作业配置异常:{0}", ex.Message));
                }
            }
        }

        /// <summary>
        /// 通过Consul刷新作业配置
        /// </summary>
        /// <param name="state"></param>
        private void RefreshJobConfigsFromConsul(object state)
        {
            lock (refreshLocker)
            {
                try
                {
                    var latestJobConfigs = LoadJobConfigsFromConsul();
                    WriteLog(string.Format("发现作业配置数量:{0}", latestJobConfigs.Count));

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

                    // 更新配置
                    for (int i = jobConfigs.Count - 1; i >= 0; i--)
                    {
                        var jobConfig = jobConfigs[i];
                        var latestJobConfig = latestJobConfigs.Where(d => d.Name == jobConfig.Name).FirstOrDefault();
                        if (latestJobConfig != null && latestJobConfig.ModifyIndex != jobConfig.ModifyIndex)
                        {
                            jobConfigs[i] = latestJobConfig;
                        }
                    }

                    // 添加新JobConfig
                    if (newJobConfigList.Count > 0)
                    {
                        foreach (var jobConfig in newJobConfigList)
                        {
                            jobConfigs.Add(jobConfig);
                            OnJobConfigJoinEventHandler?.Invoke(jobConfig);
                        }
                    }

                    // 移除删除的JobConfig
                    if (removeJobConfigList.Count > 0)
                    {
                        for (int i = jobConfigs.Count - 1; i >= 0; i--)
                        {
                            if (removeJobConfigList.Where(d => d.Name == jobConfigs[i].Name).Any())
                            {
                                var removeJobConfig = jobConfigs[i];

                                // 从内存中移除
                                var mRemoveResult = jobConfigs.Remove(removeJobConfig);
                                WriteLog("内存中移除作业配置结果：" + mRemoveResult.ToString());

                                OnJobConfigRemoveEventHandler?.Invoke(removeJobConfig);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    WriteLog(string.Format("刷新作业配置异常:{0},{1}", ex.Message, ex.StackTrace));
                }
            }
        }

        /// <summary>
        /// 尝试添加作业配置，如果作业配置不存在，则返回true，否则返回false
        /// </summary>
        /// <param name="jobConfig"></param>
        /// <returns></returns>
        private bool TryAddJobConfig(JobConfig jobConfig)
        {
            var jobConfigKey = string.Format("Swift/{0}/Jobs/{1}/Config", Name, jobConfig.Name);
            var configKV = ConsulKV.Get(jobConfigKey);
            if (configKV == null)
            {
                configKV = ConsulKV.Create(jobConfigKey);
            }

            configKV.Value = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(jobConfig));
            return ConsulKV.CAS(configKV);
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
            List<JobConfig> newJobConfigs = new List<JobConfig>();

            var jobRootPath = Path.Combine(Environment.CurrentDirectory, "Jobs");
            if (!Directory.Exists(jobRootPath))
            {
                WriteLog(string.Format("作业包目录为空，没有作业包真高兴！"));
                return null;
            }

            // 先检查新的作业包
            var jobPackages = Directory.GetFiles(jobRootPath, "*.zip");
            foreach (var pkg in jobPackages)
            {
                int fileNameStartIndex = pkg.LastIndexOf(Path.DirectorySeparatorChar) + 1;
                string pkgName = pkg.Substring(fileNameStartIndex, pkg.LastIndexOf('.') - fileNameStartIndex);
                var jobPath = Path.Combine(jobRootPath, pkgName, "config");

                if (!Directory.Exists(jobPath))
                {
                    Directory.CreateDirectory(jobPath);

                    // 只把作业配置文件取出来
                    try
                    {
                        using (var zip = ZipFile.Open(pkg, ZipArchiveMode.Read))
                        {
                            zip.GetEntry("job.json").ExtractToFile(Path.Combine(jobPath, "job.json"));
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new JobPackageConfigExtractException(string.Format("作业包[{0}]提取配置文件异常", pkgName), ex);
                    }
                }
            }

            // 然后检查所有作业目录
            var jobDirectories = Directory.GetDirectories(jobRootPath);
            foreach (var jobDir in jobDirectories)
            {
                string jobConfigPath = Path.Combine(jobDir, "config", "job.json");
                JobConfig jobConfig = new JobConfig(jobConfigPath);
                newJobConfigs.Add(jobConfig);
            }

            return newJobConfigs;
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
        /// 写日志
        /// </summary>
        /// <param name="message"></param>
        private void WriteLog(string message)
        {
            Console.WriteLine(string.Format("{0} {1}", DateTime.Now.ToString(), message));
        }

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
