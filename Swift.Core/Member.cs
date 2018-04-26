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
            communicator.OnDownloadDataCompletedHandler += ProcessDownloadDataCompleted;
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

        #region 下载数据
        /// <summary>
        /// 下载数据
        /// </summary>
        /// <param name="member"></param>
        /// <param name="msgType"></param>
        /// <param name="msgData"></param>
        public void Download(Member member, string msgType, string fileName)
        {
            Dictionary<string, string> paras = new Dictionary<string, string>();
            paras.Add("fileName", fileName);

            Download(member, msgType, paras);
        }

        /// <summary>
        /// 下载数据
        /// </summary>
        /// <param name="member"></param>
        /// <param name="msgType"></param>
        /// <param name="msgData"></param>
        public void Download(Member member, string msgType, Dictionary<string, string> paras)
        {
            if (communicator == null)
            {
                LogWriter.Write("通信器未开");
            }

            communicator.Download(member, msgType, paras);
        }

        /// <summary>
        /// 下载数据处理
        /// </summary>
        /// <param name="msgType"></param>
        /// <param name="fileName"></param>
        /// <param name="data"></param>
        private void ProcessDownloadDataCompleted(string msgType, Dictionary<string, string> paras, byte[] data)
        {
            var fileName = paras["fileName"];
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
            var fileDirectory = Path.Combine(SwiftConfiguration.BaseDirectory, fileName.Substring(0, fileName.LastIndexOf('/')));
            if (!Directory.Exists(fileDirectory))
            {
                Directory.CreateDirectory(fileDirectory);
            }

            File.WriteAllBytes(fileName, data);
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
            byte[] inputBytes = new byte[context.Request.ContentLength64];
            if (inputBytes.LongLength > 0)
            {
                var offSet = 0;
                while (offSet < inputBytes.LongLength)
                {
                    if (inputBytes.LongLength - offSet > 4096)
                    {
                        context.Request.InputStream.Read(inputBytes, offSet, 4096);
                    }
                    else
                    {
                        context.Request.InputStream.Read(inputBytes, offSet, (int)(inputBytes.LongLength - offSet));
                    }

                    offSet += 4096;
                }
            }

            // 下载作业包
            var absoluteUri = context.Request.Url.AbsolutePath;
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

            return new byte[0];
        }

        /// <summary>
        /// 处理下载请求
        /// </summary>
        private byte[] ProcessDownloadRequest(byte[] inputData, NameValueCollection paras)
        {
            var fileName = paras["fileName"];
            if (string.IsNullOrWhiteSpace(fileName))
            {
                throw new ArgumentNullException("fileName参数为空");
            }

            fileName = HttpUtility.UrlDecode(fileName);

            string filePath = Path.Combine(SwiftConfiguration.BaseDirectory, fileName.Replace('/', Path.DirectorySeparatorChar));
            LogWriter.Write(string.Format("处理下载请求:{0}", filePath));

            return File.ReadAllBytes(filePath);
        }
        #endregion

        /// <summary>
        /// 给成员发消息
        /// </summary>
        /// <param name="member"></param>
        /// <param name="msgType"></param>
        /// <param name="msgData"></param>
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
