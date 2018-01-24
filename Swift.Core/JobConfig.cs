using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Swift.Core
{
    /// <summary>
    /// 作业配置类
    /// </summary>
    public class JobConfig
    {
        /// <summary>
        /// 最后一次记录ID
        /// </summary>
        public string LastRecordId { get; set; }

        /// <summary>
        /// 最后一次记录运行开始时间
        /// </summary>
        public DateTime? LastRecordStartTime { get; set; }

        /// <summary>
        /// 作业名称
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 作业执行文件名称
        /// </summary>
        public string FileName { get; set; }

        /// <summary>
        /// 作业类名称（含命名空间）
        /// </summary>
        public string JobClassName { get; set; }

        /// <summary>
        /// 运行时间计划，格式HH:mm
        /// </summary>
        public string[] RunTimePlan { get; set; }

        /// <summary>
        /// 修改索引
        /// </summary>
        public ulong ModifyIndex { get; set; }

        /// <summary>
        /// 构造函数，无参数
        /// </summary>
        public JobConfig()
        {
        }

        /// <summary>
        /// 构造函数，使用配置文件创建作业配置类的实例
        /// </summary>
        /// <param name="physicalConfigPath">作业配置文件的物理路径</param>
        public JobConfig(string physicalConfigPath)
        {
            if (!File.Exists(physicalConfigPath))
            {
                throw new Exception(string.Format("作业配置文件不存在:{0}", physicalConfigPath));
            }

            var jobConfigJson = string.Empty;
            try
            {
                jobConfigJson = File.ReadAllText(physicalConfigPath, Encoding.UTF8);
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("读取作业配置文件异常:{0}", ex.Message));
            }

            if (string.IsNullOrWhiteSpace(jobConfigJson))
            {
                throw new Exception(string.Format("作业配置文件为空:{0}", physicalConfigPath));
            }

            var jobConfig = CreateInstance(jobConfigJson);
            if (jobConfig == null)
            {
                throw new Exception(string.Format("作业配置文件解析为null:{0}", physicalConfigPath));
            }

            LastRecordId = jobConfig.LastRecordId;
            LastRecordStartTime = jobConfig.LastRecordStartTime;
            Name = jobConfig.Name;
            FileName = jobConfig.FileName;
            JobClassName = jobConfig.JobClassName;
            RunTimePlan = jobConfig.RunTimePlan;
        }

        /// <summary>
        /// 移除作业的所有文件
        /// </summary>
        public void RemoveAllFile()
        {
            string pkgPath = Path.Combine(Environment.CurrentDirectory, "Jobs", Name);
            Directory.Delete(pkgPath, true);
        }

        /// <summary>
        /// 使用配置Json创建作业配置
        /// </summary>
        /// <param name="jobConfigJson"></param>
        /// <returns></returns>
        public static JobConfig CreateInstance(string jobConfigJson)
        {
            JobConfig jobConfig;
            try
            {
                jobConfig = JsonConvert.DeserializeObject<JobConfig>(jobConfigJson);
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("作业配置文件解析失败:{0}", ex.Message));
            }

            return jobConfig;
        }

        /// <summary>
        /// 保存作业包
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="data"></param>
        public static void SaveJobPackage(string fileName, byte[] data)
        {
            string pkgPath = Path.Combine(Environment.CurrentDirectory, "Jobs", fileName);
            File.WriteAllBytes(pkgPath, data);

            string pkgName = fileName.Replace(".zip", "");
            var jobPath = Path.Combine(Environment.CurrentDirectory, "Jobs", pkgName, "config");

            if (!Directory.Exists(jobPath))
            {
                Directory.CreateDirectory(jobPath);

                // 只把作业配置文件取出来
                using (var zip = ZipFile.Open(pkgPath, ZipArchiveMode.Read))
                {
                    zip.GetEntry("job.json").ExtractToFile(Path.Combine(jobPath, "job.json"));
                }
            }
        }
    }
}
