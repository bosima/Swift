using Consul;
using Swift.Core.Log;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Swift.Core.Consul
{
    /// <summary>
    /// Consul服务管理相关
    /// </summary>
    public static class ConsulService
    {
        private static ConsulClient client = new ConsulClient();

        /// <summary>
        /// 获取服务所有节点的健康状态
        /// </summary>
        /// <returns>The healths.</returns>
        /// <param name="serviceName">Service name.</param>
        public static Dictionary<string, bool> GetHealths(string serviceName, CancellationToken cancellationToken = default(CancellationToken))
        {
            Dictionary<string, bool> healths = new Dictionary<string, bool>();

            var services = Retry(() =>
            {
                return client.Health.Service(serviceName, cancellationToken).Result;
            }, 2);

            if (services.Response != null || services.Response.Length > 0)
            {
                var serviceEntry = services.Response;
                foreach (var service in serviceEntry)
                {
                    var serviceId = service.Service.ID;
                    var serviceStatus = !service.Checks.Any(d => d.Status != HealthStatus.Passing);
                    healths.Add(serviceId, serviceStatus);
                }
            }

            return healths;
        }

        /// <summary>
        /// 更新服务健康检查的TTL
        /// </summary>
        /// <returns></returns>
        public static void PassTTL(string serviceId)
        {
            Retry(() =>
            {
                var checkId = "CHECK:" + serviceId;
                client.Agent.PassTTL(checkId, "Alive").Wait();
            }, 2);
        }

        /// <summary>
        /// 注销服务
        /// </summary>
        /// <param name="serviceId"></param>
        public static bool DeregisterService(string serviceId)
        {
            var deRegResult = Retry(() =>
            {
                return client.Agent.ServiceDeregister(serviceId).Result;
            }, 2);

            if (deRegResult.StatusCode != System.Net.HttpStatusCode.OK)
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// 注销健康检测
        /// </summary>
        /// <param name="checkId"></param>
        /// <returns></returns>
        public static bool DeregisterServiceCheck(string checkId)
        {
            var deRegResult = Retry(() =>
            {
                return client.Agent.CheckDeregister(checkId).Result;
            }, 2);

            if (deRegResult.StatusCode != System.Net.HttpStatusCode.OK)
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// The ttl pass thread.
        /// </summary>
        private static Thread ttlPassThread;

        /// <summary>
        /// 注册服务，使用此方法注册的服务需要定时Pass TTL
        /// </summary>
        /// <param name="serviceId"></param>
        /// <param name="serviceName"></param>
        /// <param name="ttl"></param>
        /// <returns></returns>
        public static void RegisterService(string serviceId, string serviceName, int ttl)
        {
            var deRegResult = Retry(() =>
            {
                return client.Agent.ServiceDeregister(serviceId).Result;
            }, 2);

            if (deRegResult.StatusCode != System.Net.HttpStatusCode.OK)
            {
                throw new Exception("Swift Member注册前注销服务失败，返回值：" + deRegResult.StatusCode);
            }

            var regResult = Retry(() =>
            {
                return client.Agent.ServiceRegister(new AgentServiceRegistration()
                {
                    ID = serviceId,
                    Name = serviceName
                }).Result;

            }, 2);

            if (regResult.StatusCode != System.Net.HttpStatusCode.OK)
            {
                throw new Exception("Swift Member注册失败，返回值：" + regResult.StatusCode);
            }

            var regCheckResult = Retry(() =>
            {
                return client.Agent.CheckRegister(new AgentCheckRegistration()
                {
                    ID = "CHECK:" + serviceId,
                    Name = "CHECK " + serviceId,
                    DeregisterCriticalServiceAfter = new TimeSpan(1, 0, 0),
                    Notes = "服务 " + serviceId + " 健康监测",
                    ServiceID = serviceId,
                    Status = HealthStatus.Warning,
                    TTL = new TimeSpan(0, 0, ttl)
                }).Result;
            }, 2);

            if (regCheckResult.StatusCode != System.Net.HttpStatusCode.OK)
            {
                throw new Exception("Swift Member健康检查注册失败，返回值：" + regCheckResult.StatusCode);
            }

            if (ttlPassThread == null)
            {
                ttlPassThread = new Thread(new ThreadStart(() =>
                {
                    var sleepTime = (ttl / 2 - 1) * 1000;

                    while (true)
                    {
                        try
                        {
                            PassTTL(serviceId);
                        }
                        catch (Exception ex)
                        {
                            LogWriter.Write(string.Format("Consul PassTTL异常:{0}", ex.Message + ex.StackTrace));
                        }

                        Thread.Sleep(sleepTime);
                    }
                }));

                ttlPassThread.Start();
            }
        }

        private static void Retry(Action action, int retryTimes)
        {
            int i = retryTimes;
            while (i > 0)
            {
                try
                {
                    action();
                    break;
                }
                catch (AggregateException ex)
                {
                    LogWriter.Write("执行ConsulService操作异常。", ex);

                    i--;

                    if (i == 0)
                    {
                        throw;
                    }

                    Thread.Sleep(1000);
                }
            }
        }

        private static T Retry<T>(Func<T> func, int retryTimes)
        {
            int i = retryTimes;
            while (i > 0)
            {
                try
                {
                    return func();
                }
                catch (AggregateException ex)
                {
                    LogWriter.Write("执行ConsulService操作异常。", ex);

                    i--;

                    if (i == 0)
                    {
                        throw;
                    }

                    Thread.Sleep(1000);
                }
            }

            return default(T);
        }
    }
}
