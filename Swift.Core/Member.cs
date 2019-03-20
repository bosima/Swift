using Newtonsoft.Json;
using Swift.Core.Log;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Web;

namespace Swift.Core
{
    /// <summary>
    /// Swift集群成员
    /// </summary>
    public class Member
    {
        /// <summary>
        /// 成员通信器
        /// </summary>
        private MemberCommunicator _communicator;

        /// <summary>
        /// 管理员选举
        /// </summary>
        private ManagerElection _managerElection;

        /// <summary>
        /// 管理员选举监控线程
        /// </summary>
        private Thread _managerElectionWatchThread;

        /// <summary>
        /// 管理员剧本
        /// </summary>
        private ManagerPlay _managerPlay;

        /// <summary>
        /// 工人剧本
        /// </summary>
        private WorkerPlay _workerPlay;

        private readonly object _roleTransferLocker = new object();

        /// <summary>
        /// 构造函数
        /// </summary>
        public Member()
        {
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
        /// 首次注册时间
        /// </summary>
        public DateTime? FirstRegisterTime { get; set; }

        /// <summary>
        /// 上线时间
        /// </summary>
        public DateTime? OnlineTime { get; set; }

        /// <summary>
        /// 离线时间
        /// </summary>
        public DateTime? OfflineTime { get; set; }

        /// <summary>
        /// 当前所属集群
        /// </summary>
        [JsonIgnore]
        public Cluster Cluster { get; set; }

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
        /// 开张
        /// </summary>
        public void Open()
        {
            LogWriter.Write("member opening ...");

            StartCommunicator();

            Cluster.OnJobConfigJoinEventHandler += OnJobConfigJoin;
            Cluster.OnJobConfigUpdateEventHandler += OnJobConfigUpdate;
            Cluster.OnJobConfigRemoveEventHandler += OnJobConfigRemove;

            _managerElectionWatchThread = new Thread(WatchManagerElection)
            {
                Name = "ManagerElectionWatchThread",
                IsBackground = true
            };
            _managerElectionWatchThread.Start();

            LogWriter.Write("member opened");
        }

        /// <summary>
        /// 关门
        /// </summary>
        public void Close()
        {
            if (_managerElectionWatchThread != null)
            {

            }

            if (_managerPlay != null)
            {
                _managerPlay.Stop();
                _managerPlay = null;
            }

            if (_workerPlay != null)
            {
                _workerPlay.Stop();
                _workerPlay = null;
            }

            StopCommunicator();
        }

        /// <summary>
        /// 作业配置新增处理
        /// </summary>
        /// <param name="jobConfig">Job config.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private void OnJobConfigJoin(JobConfig jobConfig, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (Cluster.Manager != null && Cluster.Manager.Id != Id)
            {
                // 下载作业包
                EnsureJobPackage(jobConfig.Name, jobConfig.Version, cancellationToken);
            }
        }

        /// <summary>
        /// 作业配置更新处理
        /// </summary>
        /// <param name="jobConfig">Job config.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private void OnJobConfigUpdate(JobConfig jobConfig, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (Cluster.Manager != null && Cluster.Manager.Id != Id)
            {
                // 检查版本，如果发生变化，则下载作业包
                EnsureJobPackage(jobConfig.Name, jobConfig.Version, cancellationToken);
            }
        }

        /// <summary>
        /// 作业配置删除处理
        /// </summary>
        /// <param name="jobConfig">Job config.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private void OnJobConfigRemove(JobConfig jobConfig, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (Cluster.Manager != null && Cluster.Manager.Id != Id)
            {
                // 移除作业包
                RemoveJobConfig(jobConfig, cancellationToken);
            }
        }

        /// <summary>
        /// 监控管理员选举
        /// </summary>
        private void WatchManagerElection()
        {
            _managerElection = new ManagerElection(Cluster.Name, Id);
            _managerElection.ManagerElectCompletedEventHandler += HandleManagerElectCompleted;
            _managerElection.Watch();
        }

        /// <summary>
        /// 处理当前成员Manager选举结果
        /// </summary>
        /// <param name="result">If set to <c>true</c> result.</param>
        /// <param name="currentMemberId">Current member identifier.</param>
        private void HandleManagerElectCompleted(bool result, string currentMemberId)
        {
            LogWriter.Write(string.Format("current member elect result: {0}，New Manager is: {1}", result, currentMemberId));

            lock (_roleTransferLocker)
            {
                if (result)
                {
                    // 本机选举为Manager
                    if (_workerPlay != null)
                    {
                        _workerPlay.Stop();
                        _workerPlay = null;
                    }

                    // 剧本切换后，考虑到一些涉及成员的操作，比如作业任务重新分配，上个Manager节点可能永久下线，
                    // 应该首先执行这个健康检查，以避免进入不可用节点超时处理机制，耽误时间。
                    // 在这里放个这可能有点怪，如何处理呢？
                    Cluster.CheckMembersHealth();

                    if (_managerPlay == null)
                    {
                        _managerPlay = new ManagerPlay(this);
                        _managerPlay.Start();
                    }
                }
                else
                {
                    // 本机未能选举为Manager
                    if (_managerPlay != null)
                    {
                        _managerPlay.Stop();
                        _managerPlay = null;
                    }

                    if (_workerPlay == null)
                    {
                        _workerPlay = new WorkerPlay(this);
                        _workerPlay.Start();
                    }
                }
            }
        }

        /// <summary>
        /// 启动通信器
        /// </summary>
        protected void StartCommunicator()
        {
            _communicator = new MemberCommunicator(Id);
            _communicator.OnReceiveWebRequestHandler += ProcessRequest;
            _communicator.OnReceiveWebResponseHandler += ProcessResponse;
            _communicator.Start();
        }

        /// <summary>
        /// 停止通信器
        /// </summary>
        protected void StopCommunicator()
        {
            if (_communicator != null)
            {
                _communicator.Stop();
            }
        }

        /// <summary>
        /// 确保作业包存在，如不存在则下载
        /// </summary>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobVersion">Job version.</param>
        public void EnsureJobPackage(string jobName, string jobVersion, CancellationToken cancellationToken = default(CancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                var manager = Cluster.Manager;
                if (manager == null)
                {
                    LogWriter.Write("EnsureJobPackage->当前没有管理员", LogLevel.Warn);
                    return;
                }

                var jobPkgPath = SwiftConfiguration.GetJobPackagePath(jobName, jobVersion);
                if (!File.Exists(jobPkgPath))
                {
                    Download(manager, "download/job/package",
                       new Dictionary<string, string>
                       {
                       {"jobName",jobName},
                       {"jobVersion",jobVersion}
                       }, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                LogWriter.Write("执行任务前确保作业包存在时发生异常", ex);
                throw;
            }
        }

        /// <summary>
        /// 移除作业
        /// </summary>
        /// <param name="jobConfig">Job config.</param>
        private void RemoveJobConfig(JobConfig jobConfig, CancellationToken cancellationToken)
        {
            Cluster.RemoveJobConfig(jobConfig, cancellationToken);
        }

        #region 发起数据下载
        /// <summary>
        /// 下载数据
        /// </summary>
        /// <param name="member"></param>
        /// <param name="msgType"></param>
        /// <param name="fileName"></param>
        public void Download(Member member, string msgType, string fileName, CancellationToken cancellationToken = default(CancellationToken))
        {
            Dictionary<string, string> paras = new Dictionary<string, string>
            {
                { "fileName", fileName }
            };

            Download(member, msgType, paras, cancellationToken);
        }

        /// <summary>
        /// 下载数据
        /// </summary>
        /// <param name="member"></param>
        /// <param name="msgType"></param>
        /// <param name="paras"></param>
        public void Download(Member member, string msgType, Dictionary<string, string> paras, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (_communicator == null)
            {
                LogWriter.Write("通信器未开");
            }

            _communicator.Download(member, msgType, paras, cancellationToken);
        }

        #endregion

        #region 处理响应
        /// <summary>
        /// 下载数据处理
        /// </summary>
        /// <param name="msgType"></param>
        /// <param name="paras"></param>
        /// <param name="data"></param>
        private void ProcessResponse(string msgType, Dictionary<string, string> paras, byte[] data, CancellationToken cancellationToken = default(CancellationToken))
        {
            string filePath = string.Empty;
            paras.TryGetValue("fileName", out string fileName);
            if (!string.IsNullOrWhiteSpace(fileName))
            {
                filePath = Path.Combine(SwiftConfiguration.BaseDirectory, fileName.Replace('/', Path.DirectorySeparatorChar));
            }

            if (string.IsNullOrWhiteSpace(filePath))
            {
                if (msgType == "download/job/package")
                {
                    var jobName = paras["jobName"];
                    var jobVersion = paras["jobVersion"];
                    filePath = SwiftConfiguration.GetJobPackagePath(jobName, jobVersion);
                }

                if (msgType == "download/task/requirement")
                {
                    var jobName = paras["jobName"];
                    var jobId = paras["jobId"];
                    var taskId = int.Parse(paras["taskId"]);
                    filePath = SwiftConfiguration.GetJobTaskRequirementPath(jobName, jobId, taskId);
                }

                if (msgType == "download/task/result")
                {
                    var jobName = paras["jobName"];
                    var jobId = paras["jobId"];
                    var taskId = int.Parse(paras["taskId"]);
                    filePath = SwiftConfiguration.GetJobTaskResultPath(jobName, jobId, taskId);
                }
            }

            LogWriter.Write(string.Format("收到下载数据：{0},{1}", msgType, filePath));

            SaveDownloadFile(filePath, data, cancellationToken);
        }

        /// <summary>
        /// 保存下载文件
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="data"></param>
        private void SaveDownloadFile(string filePath, byte[] data, CancellationToken cancellationToken = default(CancellationToken))
        {
            string fileDirectory = filePath.Substring(0, filePath.LastIndexOf(Path.DirectorySeparatorChar));
            if (!Directory.Exists(fileDirectory))
            {
                Directory.CreateDirectory(fileDirectory);
            }

            File.WriteAllBytesAsync(filePath, data, cancellationToken).Wait();
            LogWriter.Write(string.Format("已保存文件：{0}", filePath));
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

            // 下载文件
            string requestUrl = context.Request.Url.AbsolutePath;
            if (requestUrl.StartsWith("/download", StringComparison.Ordinal))
            {
                return ProcessDownloadRequest(requestUrl, inputBytes, context.Request.QueryString);
            }

            // 上传文件
            if (requestUrl.StartsWith("/upload", StringComparison.Ordinal))
            {
                ProcessUploadRequest(requestUrl, inputBytes, context.Request.QueryString);
            }

            // 控制
            if (requestUrl.StartsWith("/control", StringComparison.Ordinal))
            {
                return ProcessControlRequest(requestUrl, inputBytes, context.Request.QueryString);
            }

            return new byte[0];
        }

        /// <summary>
        /// 处理下载请求
        /// </summary>
        private byte[] ProcessDownloadRequest(string requestUrl, byte[] inputData, NameValueCollection paras)
        {
            string filePath = string.Empty;

            // 如果指定了文件名，则使用文件名
            string fileName = paras["fileName"];
            if (!string.IsNullOrWhiteSpace(fileName))
            {
                fileName = HttpUtility.UrlDecode(fileName);
                filePath = Path.Combine(SwiftConfiguration.BaseDirectory, fileName.Replace('/', Path.DirectorySeparatorChar));
            }

            // 如果未指定文件名，则根据业务参数计算
            if (string.IsNullOrWhiteSpace(filePath))
            {
                if (requestUrl == "/download/task/requirement")
                {
                    var jobName = paras["jobName"];
                    var jobId = paras["jobId"];
                    var taskId = paras["taskId"];
                    filePath = SwiftConfiguration.GetJobTaskRequirementPath(jobName, jobId, int.Parse(taskId));
                }

                if (requestUrl == "/download/job/package")
                {
                    var jobName = paras["jobName"];
                    var jobVersion = paras["jobVersion"];
                    filePath = SwiftConfiguration.GetJobPackagePath(jobName, jobVersion);
                }

                if (requestUrl == "/download/task/result")
                {
                    var jobName = paras["jobName"];
                    var jobId = paras["jobId"];
                    var taskId = paras["taskId"];
                    filePath = SwiftConfiguration.GetJobTaskResultPath(jobName, jobId, int.Parse(taskId));
                }

                if (requestUrl == "/download/job/result")
                {
                    var jobName = paras["jobName"];
                    var jobId = paras["jobId"];
                    filePath = SwiftConfiguration.GetJobResultPackagePath(jobName, jobId);
                }
            }

            LogWriter.Write(string.Format("处理下载请求:{0}", filePath));

            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException("文件不存在：" + filePath);
            }

            return File.ReadAllBytes(filePath);
        }

        /// <summary>
        /// 处理上传请求
        /// </summary>
        /// <param name="requestUrl">Request URL.</param>
        /// <param name="inputData">Input data.</param>
        /// <param name="paras">Paras.</param>
        private void ProcessUploadRequest(string requestUrl, byte[] inputData, NameValueCollection paras)
        {
            string filePath = string.Empty;

            // 如果指定了文件名，则使用文件名
            string fileName = paras["fileName"];
            if (!string.IsNullOrWhiteSpace(fileName))
            {
                fileName = HttpUtility.UrlDecode(fileName);
                filePath = Path.Combine(SwiftConfiguration.BaseDirectory, fileName.Replace('/', Path.DirectorySeparatorChar));
            }

            // 如果未指定文件名，则根据业务参数计算
            if (string.IsNullOrWhiteSpace(filePath))
            {
                if (requestUrl == "/upload/job/package")
                {
                    var jobName = paras["jobName"];
                    var jobVersion = paras["jobVersion"];
                    filePath = SwiftConfiguration.GetJobPackagePath(jobName, jobVersion);
                }
            }

            LogWriter.Write(string.Format("处理上传请求:{0}", filePath));


            string direcotryPath = filePath.Substring(0, filePath.LastIndexOf(Path.DirectorySeparatorChar));
            if (!Directory.Exists(direcotryPath))
            {
                Directory.CreateDirectory(direcotryPath);
            }

            var fileLockName = SwiftConfiguration.GetFileOperateLockName(filePath);
            lock (string.Intern(fileLockName))
            {
                File.WriteAllBytes(filePath, inputData);
            }
            LogWriter.Write(string.Format("文件已经保存成功:{0}", filePath));

            ProcessAfterUploadRequest(requestUrl, filePath);
        }

        /// <summary>
        /// 处理上传请求后事务
        /// </summary>
        /// <param name="requestUrl">Request URL.</param>
        /// <param name="filePath">File path.</param>
        private void ProcessAfterUploadRequest(string requestUrl, string filePath)
        {

        }

        /// <summary>
        /// 处理控制请求
        /// </summary>
        /// <param name="requestUrl">Request URL.</param>
        /// <param name="inputData">Input data.</param>
        /// <param name="paras">Paras.</param>
        private byte[] ProcessControlRequest(string requestUrl, byte[] inputData, NameValueCollection paras)
        {
            if (requestUrl == "/control/job/record/run")
            {
                string jobName = paras["jobName"];

                LogWriter.Write(string.Format("处理运行作业请求:{0}", jobName));

                var result = Cluster.CheckAndCreateNewJobRecord(jobName, default(CancellationToken));

                return new byte[] { Convert.ToByte(result) };
            }

            if (requestUrl == "/control/job/record/cancel")
            {
                string jobName = paras["jobName"];
                string jobId = paras["jobId"];

                LogWriter.Write(string.Format("处理取消作业请求:{0},{1}", jobName, jobId));

                var result = Cluster.CancelJobRecord(jobName, jobId);

                return new byte[] { Convert.ToByte(result) };
            }

            return new byte[0];
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
            if (_communicator == null)
            {
                LogWriter.Write("通信器未开");
            }

            _communicator.SendRequest(member, msgType, Encoding.UTF8.GetBytes(msg));
        }
    }
}
