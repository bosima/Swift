using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using Swift.Core;
using Swift.Core.Election;

namespace Swift.Management.Swift
{
    public abstract class BaseSwiftService
    {

        /// <summary>
        /// 获取集群所有成员
        /// </summary>
        /// <returns>The members.</returns>
        /// <param name="clusterName">Cluster name.</param>
        public abstract List<Member> GetMembers(string clusterName);

        /// <summary>
        /// 获取集群的Manager
        /// </summary>
        /// <returns>The member.</returns>
        /// <param name="clusterName">Cluster name.</param>
        public abstract Member GetManager(string clusterName);

        /// <summary>
        /// 重启Manager选举
        /// </summary>
        /// <param name="clusterName">Cluster name.</param>
        public void RestartManagerElection(string clusterName)
        {
            var election = new ManagerElectionManager(clusterName, string.Empty, null);
            election.Reset();
        }

        /// <summary>
        /// 发布作业
        /// </summary>
        /// <returns><c>true</c>, if job was published, <c>false</c> otherwise.</returns>
        /// <param name="file">form file</param>
        public bool PublishJobPackage(string clusterName, FormFile file)
        {
            // 获取当前Manager
            var manager = GetManager(clusterName);
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

            // 设置版本为当前时间
            jobConfig.Version = DateTime.Now.ToString("yyyyMMddHHmmss");

            // 重新写配置文件
            File.WriteAllText(jobConfigPath, JsonConvert.SerializeObject(jobConfig));

            // 将新的配置文件打包
            using (var zip = ZipFile.Open(pkgPath, ZipArchiveMode.Update))
            {
                var entry = zip.GetEntry("job.json");
                entry.Delete();

                zip.CreateEntryFromFile(jobConfigPath, "job.json");
            }
            #endregion

            // 上传到Manager
            string url = string.Format("{0}upload/job/package?jobName={1}&jobVersion={2}", manager.CommunicationAddress, jobConfig.Name, jobConfig.Version);
            WebClient client = new WebClient();
            client.UploadData(url, File.ReadAllBytes(pkgPath));

            return true;
        }

        /// <summary>
        /// Downloads the job result.
        /// </summary>
        /// <returns>The job result.</returns>
        /// <param name="clusterName">Cluster name.</param>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        public byte[] DownloadJobResult(string clusterName, string jobName, string jobId)
        {
            // 获取当前Manager
            var manager = GetManager(clusterName);
            if (manager == null)
            {
                throw new Exception("没有发现在线的Manager");
            }

            var jobUrl = string.Format("{0}download/job/result?jobName={1}&jobId={2}", manager.CommunicationAddress, jobName, jobId);
            WebClient client = new WebClient();
            return client.DownloadData(jobUrl);
        }

        /// <summary>
        /// 运行作业
        /// </summary>
        /// <returns>The run.</returns>
        /// <param name="clusterName">Cluster name.</param>
        /// <param name="jobName">Job name.</param>
        public bool Run(string clusterName, string jobName)
        {
            var manager = GetManager(clusterName);
            if (manager == null)
            {
                throw new Exception("没有发现在线的Manager");
            }

            var jobUrl = string.Format("{0}control/job/record/run?jobName={1}", manager.CommunicationAddress, jobName);
            WebClient client = new WebClient();
            var result = client.DownloadData(jobUrl);
            return Convert.ToBoolean(result[0]);
        }

        /// <summary>
        /// 取消作业
        /// </summary>
        /// <returns>The cancel.</returns>
        /// <param name="clusterName">Cluster name.</param>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        public bool Cancel(string clusterName, string jobName, string jobId)
        {
            var manager = GetManager(clusterName);
            if (manager == null)
            {
                throw new Exception("没有发现在线的Manager");
            }

            var jobUrl = string.Format("{0}control/job/record/cancel?jobName={1}&jobId={2}", manager.CommunicationAddress, jobName, jobId);
            WebClient client = new WebClient();
            var result = client.DownloadData(jobUrl);
            return Convert.ToBoolean(result[0]);
        }
    }
}
