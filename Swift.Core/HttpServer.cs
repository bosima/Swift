using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Swift.Core
{
    public class HttpServer
    {
        private HttpListener listener = null;
        private int serverPort = 9631;
        private string serverIp = string.Empty;

        public HttpServer(string ip, int port)
        {
            serverIp = ip;
            serverPort = port;
        }

        /// <summary>
        /// 获取基础Url
        /// </summary>
        public string BaseUrl
        {
            get
            {
                return string.Format("http://{0}:{1}/", serverIp, serverPort);
            }
        }

        /// <summary>
        /// 启动HttpServer
        /// </summary>
        public void Start()
        {
            listener = new HttpListener();
            listener.Prefixes.Add(BaseUrl);
            listener.Start();
            WriteLog(string.Format("HttpServer已经启动：http://{0}:{1}", serverIp, serverPort));

            listener.BeginGetContext(new AsyncCallback(GetContextCallBack), listener);
        }

        /// <summary>
        /// 关闭HttpServer
        /// </summary>
        public void Stop()
        {
            listener.Close();
        }

        /// <summary>
        /// 处理具体的请求
        /// </summary>
        /// <param name="context"></param>
        protected virtual void ProcessRequest(HttpListenerContext context)
        {
        }

        /// <summary>
        ///  获取上下文回调
        /// </summary>
        /// <param name="ar"></param>
        private void GetContextCallBack(IAsyncResult ar)
        {
            try
            {
                listener = ar.AsyncState as HttpListener;
                HttpListenerContext context = listener.EndGetContext(ar);
                listener.BeginGetContext(new AsyncCallback(GetContextCallBack), listener);

                ProcessRequest(context);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        /// <summary>
        /// 写日志
        /// </summary>
        /// <param name="message"></param>
        private void WriteLog(string message)
        {
            Console.WriteLine(string.Format("{0} {1}", DateTime.Now.ToString(), message));
        }
    }
}
