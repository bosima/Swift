using Newtonsoft.Json;
using Swift.Core;
using Swift.Core.Consul;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;
using System.Web.Mvc;

namespace Swift.WebUI.Controllers
{
    public class HomeController : Controller
    {
        public ActionResult Index()
        {
            var cluster = GetClustersFromConsul();
            return View(cluster);
        }

        public ActionResult Members(string cluster)
        {
            ViewBag.ClusterName = cluster;
            var members = GetMembersFromConsul(cluster);
            return View(members);
        }

        public ActionResult Jobs(string cluster)
        {
            ViewBag.ClusterName = cluster;
            var jobConfigs = GetJobsFromConsul(cluster);
            return View(jobConfigs);
        }

        public ActionResult JobRecords(string cluster, string job)
        {
            ViewBag.ClusterName = cluster;
            ViewBag.JobName = job;
            var jobRecordss = GetJobRecordsFromConsul(cluster, job);
            return View(jobRecordss);
        }

        public ActionResult About()
        {
            ViewBag.Message = "Your application description page.";

            return View();
        }

        public ActionResult Contact()
        {
            ViewBag.Message = "Your contact page.";

            return View();
        }

        /// <summary>
        /// 获取所有作业记录
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="job"></param>
        /// <returns></returns>
        private List<JobBase> GetJobRecordsFromConsul(string cluster, string job)
        {
            List<JobBase> jobRecordList = new List<JobBase>();
            var jobRecordKeyPrefix = string.Format("Swift/{0}/Jobs/{1}/Records", cluster, job);
            var jobRecordKeys = ConsulKV.Keys(jobRecordKeyPrefix);

            if (jobRecordKeys != null && jobRecordKeys.Length > 0)
            {
                var orderedKeys = jobRecordKeys.OrderByDescending(d => d);

                foreach (var recordKey in orderedKeys)
                {
                    var jobRecordKV = ConsulKV.Get(recordKey);
                    var jobRecord = JobBase.Deserialize(Encoding.UTF8.GetString(jobRecordKV.Value), new Cluster(cluster, string.Empty));
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
        private List<JobConfig> GetJobsFromConsul(string clusterName)
        {
            List<JobConfig> newJobConfigs = new List<JobConfig>();

            var jobConfigKeyPrefix = string.Format("Swift/{0}/Jobs", clusterName);
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

        /// <summary>
        /// 从Consul获取所有集群
        /// </summary>
        /// <returns></returns>
        private List<Cluster> GetClustersFromConsul()
        {
            List<Cluster> clusterList = new List<Cluster>();
            var keys = ConsulKV.Keys(string.Format("Swift/"));
            foreach (var key in keys)
            {
                var subKey = key.TrimStart("Swift/".ToCharArray());
                var clusterName = subKey.Substring(0, subKey.IndexOf('/'));
                if (!clusterList.Where(d => d.Name == clusterName).Any())
                {
                    clusterList.Add(new Cluster(clusterName, string.Empty));
                }
            }

            return clusterList;
        }

        /// <summary>
        /// 从Consul加载集群成员
        /// </summary>
        private List<Member> GetMembersFromConsul(string clusterName)
        {
            List<Member> currentMembers = new List<Member>();
            string managerStr = ConsulKV.GetValueString(string.Format("Swift/{0}/Members/Manager", clusterName));
            var workersStr = ConsulKV.GetValueString(string.Format("Swift/{0}/Members/Worker", clusterName));

            // 添加经理
            if (!string.IsNullOrWhiteSpace(managerStr))
            {
                var manager = new Manager()
                {
                    Id = managerStr,
                    Role = EnumMemberRole.Manager,
                    Status = 0,
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

            return currentMembers;
        }
    }
}