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
    public class SwiftService : ISwiftService
    {
        /// <summary>
        /// 获取所有作业记录
        /// </summary>
        /// <param name="clusterName"></param>
        /// <param name="job"></param>
        /// <returns></returns>
        public List<JobBase> GetJobRecords(string clusterName, string job)
        {
            List<JobBase> jobRecordList = new List<JobBase>();
            var jobRecordKeyPrefix = string.Format("Swift/{0}/Jobs/{1}/Records", clusterName, job);
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
        public List<JobConfig> GetJobs(string clusterName)
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
                    var subKey = key.TrimStart("Swift/".ToCharArray());
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
        /// 从Consul加载集群成员
        /// </summary>
        public List<Member> GetMembers(string clusterName)
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
        public bool PublishJob(string clusterName, FormFile file)
        {
            // 获取当前Manager
            List<Member> members = GetMembers(clusterName);
            var manager = members.FirstOrDefault(d => d.Role == EnumMemberRole.Manager && d.Status == 1);
            if (manager == null)
            {
                throw new Exception("没有发现在线的Manager");
            }

            #region 检查作业包

            // 先保存作业包
            string pkgName = file.FileName;
            string jobName = pkgName.Substring(0, pkgName.LastIndexOf('.'));
            string uploadJobPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "uploadjobs");
            if (!Directory.Exists(uploadJobPath))
            {
                Directory.CreateDirectory(uploadJobPath);
            }

            var pkgPath = Path.Combine(uploadJobPath, pkgName);
            using (var stream = new FileStream(pkgPath, FileMode.Create))
            {
                file.CopyTo(stream);
            }

            // 然后解压作业包
            string jobPath = Path.Combine(uploadJobPath, jobName);
            if (Directory.Exists(jobPath))
            {
                // 作业包目录删除重建，以保证文件都是最新的
                Directory.Delete(jobPath, true);
                Directory.CreateDirectory(jobPath);
            }

            using (var zip = ZipFile.Open(pkgPath, ZipArchiveMode.Read))
            {
                zip.ExtractToDirectory(jobPath);
            }

            // 读取配置文件
            var jobConfigPath = Path.Combine(jobPath, "job.json");
            var jobConfig = new JobConfig(jobConfigPath);

            if (string.IsNullOrWhiteSpace(jobConfig.Name)
            || string.IsNullOrWhiteSpace(jobConfig.FileName)
            || string.IsNullOrWhiteSpace(jobConfig.JobClassName)
            || jobConfig.RunTimePlan.Length <= 0)
            {
                throw new Exception("作业配置项缺失，请检查作业名称、可执行文件名称、作业入口类、运行时间计划。");
            }

            var exePath = Path.Combine(jobPath, jobConfig.FileName);
            if (!File.Exists(exePath))
            {
                throw new Exception("作业配置指定的可执行文件不存在。");
            }

            // TODO:如果有正在执行则不能上传发布，先标记为发布状态，发布状态不能运行作业

            #endregion

            // 上传到Manager
            string url = string.Format("{0}upload/job/package?fileName=Jobs/{1}", manager.CommunicationAddress, pkgName);
            WebClient client = new WebClient();
            client.UploadData(url, File.ReadAllBytes(pkgPath));

            return true;
        }
    }
}
