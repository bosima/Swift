using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Internal;
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

        [HttpPost]
        public IActionResult Upload(IFormCollection formItems)
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

                _swift.PublishJob(clusterName, (FormFile)formFile);

                return new ObjectResult(new { error = "" });
            }
            catch (Exception ex)
            {
                return new ObjectResult(new { error = ex.Message });
            }
        }
    }
}
