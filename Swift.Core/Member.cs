using Newtonsoft.Json;
using Swift.Core.Log;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace Swift.Core
{
    /// <summary>
    /// 集群成员
    /// </summary>
    public class Member
    {
        /// <summary>
        /// 通信器
        /// </summary>
        private MemberCommunicator communicator;

        /// <summary>
        /// 构造函数
        /// </summary>
        public Member()
        {

        }

        /// <summary>
        /// 当前成员所属集群
        /// </summary>
        [JsonIgnore]
        public Cluster Cluster
        {
            get;
            set;
        }

        /// <summary>
        /// 成员标识
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// 成员状态:0下线 1在线
        /// </summary>
        public int Status { get; set; }

        /// <summary>
        /// 成员角色
        /// </summary>
        public EnumMemberRole Role { get; set; }

        /// <summary>
        /// 首次注册时间
        /// </summary>
        public DateTime? FirstRegisterTime
        {
            get;
            set;
        }

        /// <summary>
        /// 上线时间
        /// </summary>
        public DateTime? OnlineTime
        {
            get;
            set;
        }

        /// <summary>
        /// 离线时间
        /// </summary>
        public DateTime? OfflineTime
        {
            get;
            set;
        }

        /// <summary>
        /// 获取通信器地址
        /// </summary>
        [JsonIgnore]
        public string CommunicationAddress
        {
            get
            {
                return string.Format("http://{0}:{1}/", Id, "9631");
            }
        }

        /// <summary>
        /// 开始处理
        /// </summary>
        protected virtual void Start()
        {
        }

        /// <summary>
        /// 停止处理
        /// </summary>
        protected virtual void Stop()
        {
        }

        /// <summary>
        /// 开张
        /// </summary>
        public void Open()
        {
            StartCommunicator();
            Start();
        }

        /// <summary>
        /// 关门
        /// </summary>
        public void Close()
        {
            StopCommunicator();
            Stop();
        }

        /// <summary>
        /// 启动通信器
        /// </summary>
        protected void StartCommunicator()
        {
            communicator = new MemberCommunicator(Id);
            communicator.OnReceiveWebRequestHandler += ProcessRequest;
            communicator.OnReceiveWebResponseHandler += ProcessResponse;
            communicator.Start();
        }

        /// <summary>
        /// 停止通信器
        /// </summary>
        protected void StopCommunicator()
        {
            if (communicator != null)
            {
                communicator.Stop();
            }
        }

        #region 发起数据下载
        /// <summary>
        /// 下载数据
        /// </summary>
        /// <param name="member"></param>
        /// <param name="msgType"></param>
        /// <param name="fileName"></param>
        public void Download(Member member, string msgType, string fileName)
        {
            Dictionary<string, string> paras = new Dictionary<string, string>
            {
                { "fileName", fileName }
            };

            Download(member, msgType, paras);
        }

        /// <summary>
        /// 下载数据
        /// </summary>
        /// <param name="member"></param>
        /// <param name="msgType"></param>
        /// <param name="paras"></param>
        public void Download(Member member, string msgType, Dictionary<string, string> paras)
        {
            if (communicator == null)
            {
                LogWriter.Write("通信器未开");
            }

            communicator.Download(member, msgType, paras);
        }

        #endregion

        #region 处理响应
        /// <summary>
        /// 下载数据处理
        /// </summary>
        /// <param name="msgType"></param>
        /// <param name="paras"></param>
        /// <param name="data"></param>
        private void ProcessResponse(string msgType, Dictionary<string, string> paras, byte[] data)
        {
            string fileName = paras["fileName"];
            LogWriter.Write(string.Format("收到下载数据：{0},{1}", msgType, fileName));

            if (msgType == "download/job/package")
            {
                SaveDownloadFile(fileName, data);
            }

            if (msgType == "download/task/requirement")
            {
                SaveDownloadFile(fileName, data);
            }

            if (msgType == "download/task/result")
            {
                SaveDownloadFile(fileName, data);
            }
        }

        /// <summary>
        /// 保存下载文件
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="data"></param>
        private void SaveDownloadFile(string fileName, byte[] data)
        {
            string fileDirectory = Path.Combine(SwiftConfiguration.BaseDirectory, fileName.Substring(0, fileName.LastIndexOf('/')));
            if (!Directory.Exists(fileDirectory))
            {
                Directory.CreateDirectory(fileDirectory);
            }

            string filePath = Path.Combine(fileDirectory, fileName.Substring(fileName.LastIndexOf('/') + 1));
            File.WriteAllBytes(filePath, data);
        }
        #endregion

        #region 处理请求
        /// <summary>
        /// 处理接收到的Web请求
        /// </summary>
        /// <param name="context"></param>
        private byte[] ProcessRequest(System.Net.HttpListenerContext context)
        {
            LogWriter.Write(string.Format("收到请求：{0}", context.Request.RawUrl));

            // 输入数据流
            var contentLength = context.Request.ContentLength64;
            MemoryStream ms = new MemoryStream();
            if (contentLength > 0)
            {
                int actual = 0;
                byte[] buffer = new byte[4096];
                while ((actual = context.Request.InputStream.Read(buffer, 0, 4096)) > 0)
                {
                    ms.Write(buffer, 0, actual);
                }
            }

            ms.Position = 0;
            byte[] inputBytes = ms.ToArray();

            // 下载作业包
            string absoluteUri = context.Request.Url.AbsolutePath;
            if (absoluteUri == "/download/job/package")
            {
                // Jobs/{0}.zip
                return ProcessDownloadRequest(inputBytes, context.Request.QueryString);
            }

            // 下载任务需求
            if (absoluteUri == "/download/task/requirement")
            {
                // Jobs/{0}/records/{1}/tasks/{2}/requirement.txt
                return ProcessDownloadRequest(inputBytes, context.Request.QueryString);
            }

            // 下载任务结果
            if (absoluteUri == "/download/task/result")
            {
                // Jobs/{0}/records/{1}/tasks/{2}/result.txt
                return ProcessDownloadRequest(inputBytes, context.Request.QueryString);
            }

            // 接收上传的作业包
            if (absoluteUri == "/upload/job/package")
            {
                // Jobs/{0}.zip
                ProcessUploadRequest(inputBytes, context.Request.QueryString);
            }

            return new byte[0];
        }

        /// <summary>
        /// 处理下载请求
        /// </summary>
        private byte[] ProcessDownloadRequest(byte[] inputData, NameValueCollection paras)
        {
            const string ParamName = "fileName";

            string fileName = paras[ParamName];
            if (string.IsNullOrWhiteSpace(fileName))
            {
                throw new ArgumentNullException(ParamName, "文件名为空");
            }

            fileName = HttpUtility.UrlDecode(fileName);

            string filePath = Path.Combine(SwiftConfiguration.BaseDirectory, fileName.Replace('/', Path.DirectorySeparatorChar));
            LogWriter.Write(string.Format("处理下载请求:{0}", filePath));

            return File.ReadAllBytes(filePath);
        }

        /// <summary>
        /// 处理上传请求
        /// </summary>
        private void ProcessUploadRequest(byte[] inputData, NameValueCollection paras)
        {
            const string ParamName = "fileName";

            string fileName = paras[ParamName];
            if (string.IsNullOrWhiteSpace(fileName))
            {
                throw new ArgumentNullException(ParamName, "文件名为空");
            }

            fileName = HttpUtility.UrlDecode(fileName);

            string filePath = Path.Combine(SwiftConfiguration.BaseDirectory, fileName.Replace('/', Path.DirectorySeparatorChar));
            LogWriter.Write(string.Format("处理上传请求:{0}", filePath));

            if (fileName.Contains('/'))
            {
                string direcotryPath = Path.Combine(SwiftConfiguration.BaseDirectory, fileName.Substring(0, fileName.LastIndexOf('/')).Replace('/', Path.DirectorySeparatorChar));
                if (!Directory.Exists(direcotryPath))
                {
                    Directory.CreateDirectory(direcotryPath);
                }
            }

            var fileLockName = SwiftConfiguration.GetFileOperateLockName(filePath);
            lock (string.Intern(fileLockName))
            {
                File.WriteAllBytes(filePath, inputData);
            }
        }
        #endregion

        /// <summary>
        /// 给成员发消息
        /// </summary>
        /// <param name="member"></param>
        /// <param name="msgType"></param>
        /// <param name="msg"></param>
        public void SendMessage(Member member, string msgType, string msg)
        {
            if (communicator == null)
            {
                LogWriter.Write("通信器未开");
            }

            communicator.SendRequest(member, msgType, Encoding.UTF8.GetBytes(msg));
        }
    }
}
