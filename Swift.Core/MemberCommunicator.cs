﻿using Swift.Core.Log;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.NetworkInformation;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;

namespace Swift.Core
{
    /// <summary>
    /// 接收Web请求事件
    /// </summary>
    /// <param name="context"></param>
    public delegate byte[] ReceiveWebRequestEvent(HttpListenerContext context);

    /// <summary>
    /// 下载数据事件
    /// </summary>
    /// <param name="msgType"></param>
    /// <param name="data"></param>
    public delegate void ReceiveWebResponseEvent(string msgType, Dictionary<string, string> paras, byte[] data, CancellationToken cancellationToken);

    /// <summary>
    /// 成员通信器
    /// </summary>
    public class MemberCommunicator : HttpServer
    {
        public MemberCommunicator(string id)
            : base(id, 9631)
        {
        }

        /// <summary>
        /// 当接收到Web请求时的处理器
        /// </summary>
        public event ReceiveWebRequestEvent OnReceiveWebRequestHandler;

        /// <summary>
        /// 当接收到Web响应时的处理器
        /// </summary>
        public event ReceiveWebResponseEvent OnReceiveWebResponseHandler;

        /// <summary>
        /// 获取通信Url
        /// </summary>
        public string CommunicationAddress
        {
            get
            {
                return base.BaseUrl;
            }
        }

        /// <summary>
        /// 启动通信器
        /// </summary>
        public new void Start()
        {
            base.Start();
        }

        /// <summary>
        /// 关闭通信器
        /// </summary>
        public new void Stop()
        {
            base.Stop();
        }

        /// <summary>
        /// 给成员发请求
        /// </summary>
        /// <param name="member"></param>
        /// <param name="msgType"></param>
        /// <param name="msgData"></param>
        public void SendRequest(Member member, string msgType, byte[] msgData)
        {
            string url = string.Format("{0}{1}", member.CommunicationAddress, msgType);
            LogWriter.Write("通信路径：" + url);
            LogWriter.Write(string.Format("数据大小：{0}", msgData.LongLength));

            // TODO:使用HttpClient更多可以自定义
            // TODO:重试3次，如果还不行则抛出异常

            WebClient client = new WebClient();
            var result = client.UploadData(url, msgData);
            var resultStr = Encoding.UTF8.GetString(result);

            var response = Newtonsoft.Json.JsonConvert.DeserializeObject<CommunicationResponse>(resultStr);
            if (response.ErrCode == 0)
            {
                LogWriter.Write("消息发送成功！");
            }
            else
            {
                LogWriter.Write("消息发送失败:" + response.ErrMsg);
            }
        }

        /// <summary>
        /// 下载数据
        /// </summary>
        /// <param name="member"></param>
        /// <param name="msgType"></param>
        public void Download(Member member, string msgType, Dictionary<string, string> paras, CancellationToken cancellationToken = default(CancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var paraStr = string.Empty;
            if (paras != null && paras.Count > 0)
            {
                foreach (var paraKey in paras.Keys)
                {
                    paraStr += string.Format("&{0}={1}", paraKey, HttpUtility.UrlEncode(paras[paraKey], Encoding.UTF8));
                }
            }

            string url = string.Format("{0}{1}{2}", member.CommunicationAddress, msgType,
                (!string.IsNullOrWhiteSpace(paraStr) ? "?" + paraStr.TrimStart('&') : string.Empty));

            LogWriter.Write("通信路径：" + url);

            int tryTimes = 2;
            while (tryTimes > 0)
            {
                try
                {
                    WebClient client = new WebClient();
                    var downloadTask = client.DownloadDataTaskAsync(new Uri(url));
                    byte[] result;

                    using (cancellationToken.Register(client.CancelAsync))
                    {
                        result = downloadTask.Result;
                    }

                    // 也许注释掉的方法更好
                    //while (!downloadTask.Wait(TimeSpan.FromSeconds(0.2)))
                    //{
                    //    cancellationToken.ThrowIfCancellationRequested();
                    //}
                    //result = downloadTask.Result;

                    OnReceiveWebResponseHandler?.Invoke(msgType, paras, result, cancellationToken);
                    break;
                }
                catch (Exception ex)
                {
                    tryTimes--;
                    if (tryTimes <= 0)
                    {
                        LogWriter.Write("下载文件处理异常", ex);
                        throw;
                    }

                    LogWriter.Write("下载文件处理异常", ex, LogLevel.Warn);

                }
            }
        }

        /// <summary>
        /// 处理通信请求
        /// </summary>
        /// <param name="context"></param>
        protected override void ProcessRequest(HttpListenerContext context)
        {
            byte[] processResult;

            try
            {
                processResult = OnReceiveWebRequestHandler?.Invoke(context);
                context.Response.StatusCode = 200;
            }
            catch (FileNotFoundException ex)
            {
                context.Response.StatusCode = 404;
                processResult = Encoding.UTF8.GetBytes("{\"ErrCode\":1,\"ErrMsg\":\"" + ex.Message + "\"}");
            }
            catch (DirectoryNotFoundException ex)
            {
                context.Response.StatusCode = 404;
                processResult = Encoding.UTF8.GetBytes("{\"ErrCode\":1,\"ErrMsg\":\"" + ex.Message + "\"}");
            }
            catch (Exception ex)
            {
                context.Response.StatusCode = 500;
                processResult = Encoding.UTF8.GetBytes("{\"ErrCode\":1,\"ErrMsg\":\"" + ex.Message + "\"}");
            }

            HttpListenerRequest request = context.Request;
            using (BinaryWriter writer = new BinaryWriter(context.Response.OutputStream))
            {
                writer.Write(processResult);
                writer.Close();
                context.Response.Close();
            }
        }

        /// <summary>
        /// 检查端口是否在使用
        /// </summary>
        /// <returns><c>true</c>, if port in use was checked, <c>false</c> otherwise.</returns>
        /// <param name="port">Port.</param>
        public static bool CheckPortInUse(int port)
        {
            bool inUse = false;

            IPGlobalProperties ipProperties = IPGlobalProperties.GetIPGlobalProperties();
            IPEndPoint[] ipEndPoints = ipProperties.GetActiveTcpListeners();

            foreach (IPEndPoint endPoint in ipEndPoints)
            {
                if (endPoint.Port == port)
                {
                    inUse = true;
                    break;
                }
            }

            return inUse;
        }
    }
}
