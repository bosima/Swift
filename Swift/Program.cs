using Swift.Core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Swift
{
    class Program
    {
        /// <summary>
        /// 当前集群
        /// </summary>
        static Cluster cluster;

        static void Main(string[] args)
        {
            // 获取启动参数
            var paras = ResolveArguments(args);

            // 检查参数错误
            List<string> errorMessages = CheckArgumentsError(paras);
            if (errorMessages != null && errorMessages.Count > 0)
            {
                ShowMessage(errorMessages);
                Environment.Exit(1);
            }

            ShowMessage("参数检查通过。");

            // 加载集群配置
            string bindingIP = string.Empty;
            if (paras.ContainsKey("-b"))
            {
                bindingIP = paras["-b"];
            }

            cluster = new Cluster(paras["-c"], bindingIP);
            cluster.Init();
            ShowMessage("集群配置加载完毕。");

            var currentMember = cluster.RegisterMember(cluster.LocalIP);
            ShowMessage("已注册到配置中心。");

            if (currentMember == null)
            {
                ShowMessage("注册失败。", true);
            }

            currentMember.Open();

            if (Console.In is StreamReader)
            {
                Console.WriteLine("Run In Interactive");

                var exit = Console.ReadLine();
                while (exit != "exit")
                {
                    exit = Console.ReadLine();
                }

                ShowMessage("当前成员准备停止工作...");
                currentMember.Close();
            }
            else
            {
                Console.WriteLine("Run In Background");
            }
        }

        /// <summary>
        /// 显示信息
        /// </summary>
        /// <param name="message"></param>
        /// <param name="isStop"></param>
        private static void ShowMessage(string message, bool isStop = false)
        {
            ShowMessage(new string[] { message }, isStop);
        }

        /// <summary>
        /// 显示信息
        /// </summary>
        /// <param name="messages"></param>
        /// <param name="isStop"></param>
        private static void ShowMessage(IEnumerable<string> messages, bool isStop = false)
        {
            if (messages.Any())
            {
                foreach (var msg in messages)
                {

                    Console.WriteLine(string.Format("{0} {1}", DateTime.Now.ToString(), msg));
                }

                if (isStop)
                {
                    Console.WriteLine("程序已停止运行，按任意键关闭窗口....");
                    Console.ReadKey();
                    Environment.Exit(0);
                }
            }
        }

        /// <summary>
        /// 检查启动参数错误
        /// </summary>
        /// <param name="paras"></param>
        /// <returns></returns>
        private static List<string> CheckArgumentsError(Dictionary<string, string> paras)
        {
            List<string> errorMessages = new List<string>();

            if (!paras.ContainsKey("-c"))
            {
                errorMessages.Add(ProcessError(2));
            }

            // TODO: 以后可以让用户自定义端口
            if (MemberCommunicator.CheckPortInUse(9631))
            {
                errorMessages.Add(ProcessError(3));
            }

            return errorMessages;
        }

        /// <summary>
        /// 处理错误
        /// </summary>
        /// <param name="errorCode"></param>
        private static string ProcessError(int errorCode)
        {
            var errorMessage = string.Empty;

            switch (errorCode)
            {
                case 2:
                    errorMessage = "未指定集群，无法启动。";
                    break;
                case 3:
                    errorMessage = "端口9631被占用了。";
                    break;
            }

            return errorMessage;
        }

        /// <summary>
        /// 解析启动参数
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        private static Dictionary<string, string> ResolveArguments(string[] args)
        {
            Dictionary<string, string> paras = new Dictionary<string, string>();

            for (int i = 0; i < args.Length; i++)
            {
                if (!args[i].StartsWith("-", StringComparison.Ordinal))
                {
                    continue;
                }

                var key = args[i].ToLower();

                if (string.IsNullOrWhiteSpace(key))
                {
                    continue;
                }

                var val = string.Empty;
                if (i + 1 < args.Length)
                {
                    if (!args[i + 1].StartsWith("-", StringComparison.Ordinal))
                    {
                        val = args[i + 1].Trim();
                        i = i + 1;
                    }
                }

                paras.Add(key, val);
            }

            return paras;
        }
    }
}
