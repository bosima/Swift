using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Swift.Management.Swift;

namespace Swift.Management.Controllers
{
    public class JobController : Controller
    {
        private readonly ISwiftService _swift;

        public JobController(ISwiftService swift)
        {
            _swift = swift;
        }

        [HttpGet]
        public IActionResult GetJobConfig(string clusterName, string jobName)
        {
            try
            {
                var result = _swift.GetJobConfig(clusterName, jobName);
                if (result != null)
                {
                    return new ObjectResult(new { errCode = 0, data = result });
                }
                else
                {
                    return new ObjectResult(new { errCode = 1, errMessage = "获取作业配置信息失败" });
                }
            }
            catch (Exception ex)
            {
                return new ObjectResult(new { errCode = 2, errMessage = ex.Message });
            }
        }

        [HttpGet]
        public IActionResult Run(string clusterName, string jobName)
        {
            try
            {
                var result = _swift.Run(clusterName, jobName);
                if (result)
                {
                    return new ObjectResult(new { errCode = 0 });
                }
                else
                {
                    return new ObjectResult(new { errCode = 1, errMessage = "作业运行失败，可能存在正在运行的作业" });
                }
            }
            catch (Exception ex)
            {
                return new ObjectResult(new { errCode = 2, errMessage = ex.Message });
            }
        }

        [HttpGet]
        public IActionResult Cancel(string clusterName, string jobName, string jobId)
        {
            try
            {
                var result = _swift.Cancel(clusterName, jobName, jobId);
                if (result)
                {
                    return new ObjectResult(new { errCode = 0 });
                }
                else
                {
                    return new ObjectResult(new { errCode = 1, errMessage = "作业取消失败，可能存在权限问题" });
                }
            }
            catch (Exception ex)
            {
                return new ObjectResult(new { errCode = 2, errMessage = ex.Message });
            }
        }

        /// <summary>
        /// 上传作业包
        /// </summary>
        /// <returns>The upload.</returns>
        /// <param name="formItems">Form items.</param>
        [HttpPost]
        public IActionResult UploadPackage(IFormCollection formItems)
        {
            try
            {
                var formFile = formItems.Files[0];
                var clusterName = formItems["clustername"][0];
                var extension = formFile.FileName.Substring(formFile.FileName.LastIndexOf('.'));

                if (extension != ".zip")
                {
                    return new ObjectResult(new { error = "不支持" + extension + "类型的文件" });
                }

                _swift.PublishJobPackage(clusterName, (FormFile)formFile);

                return new ObjectResult(new { error = "" });
            }
            catch (Exception ex)
            {
                return new ObjectResult(new { error = ex.Message });
            }
        }

        /// <summary>
        /// 下载作业结果
        /// </summary>
        /// <returns>The result.</returns>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        [HttpGet]
        public IActionResult DownloadResult(string clusterName, string jobName, string jobId)
        {
            try
            {
                var data = _swift.DownloadJobResult(clusterName, jobName, jobId);
                return File(data, "application/zip", "result.zip");
            }
            catch (Exception ex)
            {
                return File(Encoding.UTF8.GetBytes("下载文件异常:" + ex.Message), "text/plain", "error.txt");
            }
        }
    }
}
