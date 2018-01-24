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
        public static bool CheckHealth(string serviceName)
        {
            var hasHealth = false;
            using (var client = new ConsulClient())
            {
                var serviceEntry = client.Health.Service(serviceName).Result;

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
    }
}
