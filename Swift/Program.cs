using Swift.Core;
using System;
using System.Collections.Generic;
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
            ShowMessage("开始参数检查...");
            List<string> errorMessages = CheckArgumentsError(paras);
            ShowMessage(errorMessages, true);
            ShowMessage("参数检查通过。");

            // 加载集群配置
            ShowMessage("开始加载集群配置...");

            string bindingIP = string.Empty;
            if (paras.ContainsKey("-b"))
            {
                bindingIP = paras["-b"];
            }

            cluster = new Cluster(paras["-c"], bindingIP);
            cluster.Init();
            ShowMessage("集群配置加载完毕。");

            // 获取当前成员Id
            var currentMemberId = cluster.LocalIP;

            // 当前成员角色处理
            var memberRole = paras["-r"];
            Member currentMember = null;
            if (memberRole == "manager")
            {
                ShowMessage("准备注册为Manager...");
                currentMember = cluster.RegisterMember(currentMemberId, EnumMemberRole.Manager);
            }
            else if (memberRole == "worker")
            {
                ShowMessage("准备注册为Worker...");
                currentMember = cluster.RegisterMember(currentMemberId, EnumMemberRole.Worker);
            }

            if (currentMember == null)
            {
                ShowMessage("注册失败", true);
            }

            currentMember.Open();

            var exit = Console.ReadLine();
            while (exit != "exit")
            {
                exit = Console.ReadLine();
            }

            ShowMessage("当前成员准备停止工作...");
            currentMember.Close();
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
            if (!paras.ContainsKey("-r"))
            {
                errorMessages.Add(ProcessError(1));
            }

            if (!paras.ContainsKey("-c"))
            {
                errorMessages.Add(ProcessError(2));
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
                case 1:
                    errorMessage = "未指定成员角色，无法启动。";
                    break;
                case 2:
                    errorMessage = "未指定集群，无法启动。";
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
                if (!args[i].StartsWith("-"))
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
                    if (!args[i + 1].StartsWith("-"))
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
