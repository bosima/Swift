using Consul;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Swift.Core.Consul
{
    /// <summary>
    /// Consul服务管理相关
    /// </summary>
    public class ConsulService
    {
        /// <summary>
        /// 检查服务健康状态
        /// </summary>
        /// <param name="serviceId"></param>
        /// <returns></returns>
        public static bool CheckHealth(string serviceId)
        {
            var hasHealth = false;
            using (var client = new ConsulClient())
            {
                var serviceEntry = client.Health.Service(serviceId).Result;

                if (serviceEntry.Response != null && serviceEntry.Response.Length > 0)
                {
                    Array.ForEach(serviceEntry.Response, c1 =>
                   {
                       // 只要有一个检测不通过，就是不能用
                       var checkPass = c1.Checks.Any(c => c.Status != HealthStatus.Passing);
                       if (!checkPass)
                       {
                           hasHealth = true;
                       }
                   });
                }
            }

            return hasHealth;
        }

        /// <summary>
        /// 更新服务健康检查的TTL
        /// </summary>
        /// <returns></returns>
        public static void PassTTL(string serviceId)
        {
            using (var client = new ConsulClient())
            {
                var checkId = "CHECK-" + serviceId;
                client.Agent.PassTTL(checkId, "Alive").Wait();
            }
        }

        /// <summary>
        /// 注销服务
        /// </summary>
        /// <param name="serviceId"></param>
        public static bool DeregisterService(string serviceId)
        {
            using (var client = new ConsulClient())
            {
                var deRegResult = client.Agent.ServiceDeregister(serviceId).Result;
                if (deRegResult.StatusCode != System.Net.HttpStatusCode.OK)
                {
                    return false;
                }
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
            using (var client = new ConsulClient())
            {
                var deRegResult = client.Agent.CheckDeregister(checkId).Result;
                if (deRegResult.StatusCode != System.Net.HttpStatusCode.OK)
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// 注册服务，使用此方法注册的服务需要定时UpdateTTL
        /// </summary>
        /// <param name="serviceId"></param>
        /// <param name="serviceName"></param>
        /// <param name="serviceAddress"></param>
        /// <param name="ttl"></param>
        /// <returns></returns>
        public static void RegisterService(string serviceId, string serviceName, int ttl)
        {
            using (var client = new ConsulClient())
            {
                var deRegResult = client.Agent.ServiceDeregister(serviceId).Result;
                if (deRegResult.StatusCode != System.Net.HttpStatusCode.OK)
                {
                    throw new Exception("注册前的注销失败，返回值：" + deRegResult.StatusCode);
                }

                var registion = new AgentServiceRegistration()
                {
                    ID = serviceId,
                    Name = serviceName,
                };

                var regResult = client.Agent.ServiceRegister(registion).Result;
                if (regResult.StatusCode != System.Net.HttpStatusCode.OK)
                {
                    throw new Exception("注册失败，返回值：" + regResult.StatusCode);
                }

                var check = new AgentCheckRegistration()
                {
                    ID = "CHECK-" + serviceId,
                    Name = "CHECK " + serviceName,
                    TTL = new TimeSpan(0, 0, ttl),
                    DeregisterCriticalServiceAfter = new TimeSpan(1, 0, 0),
                    Notes = "服务 " + serviceName + " 健康监测",
                };

                var regCheckResult = client.Agent.CheckRegister(check).Result;
                if (regCheckResult.StatusCode != System.Net.HttpStatusCode.OK)
                {
                    throw new Exception("注册健康检查失败，返回值：" + regCheckResult.StatusCode);
                }
            }
        }
    }
}
