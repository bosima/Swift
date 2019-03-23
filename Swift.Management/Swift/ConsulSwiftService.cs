using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using Consul;
using Microsoft.AspNetCore.Http.Internal;
using Newtonsoft.Json;
using Swift.Core;
using Swift.Core.Consul;
using Swift.Core.ExtensionException;

namespace Swift.Management.Swift
{
    public class ConsulSwiftService : BaseSwiftService, ISwiftService
    {
        /// <summary>
        /// Gets the last day job records.
        /// </summary>
        /// <returns>The last day job records.</returns>
        /// <param name="clusterName">Cluster name.</param>
        /// <param name="jobName">Job name.</param>
        /// <param name="date">Date.</param>
        public List<JobBase> GetJobRecords(string clusterName, string jobName, DateTime? date)
        {
            // 如果没有指定日期则从配置中心获取最后一次作业记录时间，默认显示这一天的作业记录
            if (!date.HasValue)
            {
                var jobConfigKey = string.Format("Swift/{0}/Jobs/{1}/Config", clusterName, jobName);
                var configKV = ConsulKV.Get(jobConfigKey);
                if (configKV != null && configKV.Value != null)
                {
                    JobConfig jobConfig = JsonConvert.DeserializeObject<JobConfig>(Encoding.UTF8.GetString(configKV.Value));
                    date = jobConfig.LastRecordCreateTime;
                }
            }

            // 如果还是没有日期，则认为作业从来都没有运行过
            if (!date.HasValue)
            {
                return null;
            }

            return GetSpecifiedDayJobRecords(clusterName, jobName, date.Value);
        }

        /// <summary>
        /// 获取所有作业记录
        /// </summary>
        /// <param name="clusterName"></param>
        /// <param name="job"></param>
        /// <returns></returns>
        private List<JobBase> GetSpecifiedDayJobRecords(string clusterName, string job, DateTime date)
        {
            string year = date.ToString("yyyy");
            string month = date.ToString("MM");
            string day = date.ToString("dd");

            List<JobBase> jobRecordList = new List<JobBase>();
            var jobRecordKeyPrefix = string.Format("Swift/{0}/Jobs/{1}/Records/{2}/{3}/{4}", clusterName, job, year, month, day);
            var jobRecordKeys = ConsulKV.Keys(jobRecordKeyPrefix);

            if (jobRecordKeys != null && jobRecordKeys.Length > 0)
            {
                var orderedKeys = jobRecordKeys.OrderByDescending(d => d);

                foreach (var recordKey in orderedKeys)
                {
                    var jobRecordKV = ConsulKV.Get(recordKey);
                    var jobRecord = JobBase.Deserialize(Encoding.UTF8.GetString(jobRecordKV.Value), new Cluster(clusterName, string.Empty));
                    jobRecordList.Add(jobRecord);
                }
            }

            return jobRecordList;
        }

        /// <summary>
        /// 获取所有作业配置
        /// </summary>
        /// <param name="clusterName"></param>
        /// <returns></returns>
        public List<JobConfig> GetJobConfigs(string clusterName)
        {
            List<JobConfig> newJobConfigs = new List<JobConfig>();

            var jobConfigKeyPrefix = string.Format("Swift/{0}/Jobs", clusterName);
            var jobKeys = ConsulKV.Keys(jobConfigKeyPrefix);
            var jobConfigKeys = jobKeys?.Where(d => d.EndsWith("Config", StringComparison.Ordinal));

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

        /// <summary>
        /// 从Consul获取所有集群
        /// </summary>
        /// <returns></returns>
        public List<Cluster> GetClusters()
        {
            List<Cluster> clusterList = new List<Cluster>();
            var keys = ConsulKV.Keys(string.Format("Swift/"));
            if (keys != null && keys.Length > 0)
            {
                foreach (var key in keys)
                {
                    var subKey = key.Substring(key.IndexOf('/') + 1);
                    var clusterName = subKey.Substring(0, subKey.IndexOf('/'));
                    if (!clusterList.Any(d => d.Name == clusterName))
                    {
                        clusterList.Add(new Cluster(clusterName, string.Empty));
                    }
                }
            }

            return clusterList;
        }

        /// <summary>
        /// 从Consul获取Manager
        /// </summary>
        public override Member GetManager(string clusterName)
        {
            var managerKey = string.Format("/Swift/{0}/Manager", clusterName);
            var manager = ConsulKV.Get(managerKey);
            if (manager == null || string.IsNullOrWhiteSpace(manager.Session))
            {
                return null;
            }

            var managerId = Encoding.UTF8.GetString(manager.Value);

            return GetMembers(clusterName).FirstOrDefault(d => d.Id == managerId);
        }

        /// <summary>
        /// 从Consul加载集群成员
        /// </summary>
        public override List<Member> GetMembers(string clusterName)
        {
            var memberKey = string.Format("Swift/{0}/Members", clusterName);
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

            var serviceName = string.Format("Swift-{0}-Member", clusterName);
            var healths = ConsulService.GetHealths(serviceName);
            List<MemberWrapper> needRemoveList = new List<MemberWrapper>();
            foreach (var configMember in configMemberList)
            {
                var serviceId = string.Format("Swift-{0}-Member-{1}", clusterName, configMember.Id);
                healths.TryGetValue(serviceId, out bool isHealth);
                configMember.Status = isHealth ? 1 : 0;
            }

            return configMemberList.Select(d => d.ConvertToBase()).ToList();
        }

        /// <summary>
        /// 发布作业
        /// </summary>
        /// <returns><c>true</c>, if job was published, <c>false</c> otherwise.</returns>
        /// <param name="file">form file</param>
        public new bool PublishJobPackage(string clusterName, FormFile file)
        {
            return base.PublishJobPackage(clusterName, file);
        }

        /// <summary>
        /// Downloads the job result.
        /// </summary>
        /// <returns>The job result.</returns>
        /// <param name="clusterName">Cluster name.</param>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        public new byte[] DownloadJobResult(string clusterName, string jobName, string jobId)
        {
            return base.DownloadJobResult(clusterName, jobName, jobId);
        }

        /// <summary>
        /// Run the specified clusterName and jobName.
        /// </summary>
        /// <returns>The run.</returns>
        /// <param name="clusterName">Cluster name.</param>
        /// <param name="jobName">Job name.</param>
        public new bool Run(string clusterName, string jobName)
        {
            return base.Run(clusterName, jobName);
        }

        /// <summary>
        /// Cancel the specified job record
        /// </summary>
        /// <returns>The cancel.</returns>
        /// <param name="clusterName">Cluster name.</param>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        public new bool Cancel(string clusterName, string jobName, string jobId)
        {
            return base.Cancel(clusterName, jobName, jobId);
        }
    }
}
