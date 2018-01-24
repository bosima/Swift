using Consul;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Swift.Core.Consul
{
    /// <summary>
    /// Consul KV相关
    /// </summary>
    public class ConsulKV
    {
        /// <summary>
        /// 创建一个KVPair实例
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static KVPair Create(string key)
        {
            return new KVPair(key);
        }

        /// <summary>
        /// 获取对应Key的字符串值
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static string GetValueString(string key)
        {
            using (var client = new ConsulClient())
            {
                var kvPair = client.KV.Get(key).Result;
               
                if (kvPair.Response != null && kvPair.Response.Value != null)
                {
                    return Encoding.UTF8.GetString(kvPair.Response.Value, 0, kvPair.Response.Value.Length);
                }

                return string.Empty;
            }
        }

        /// <summary>
        /// 获取对应Key的字符串值
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static KVPair Get(string key)
        {
            using (var client = new ConsulClient())
            {
                return client.KV.Get(key).Result.Response;
            }
        }

        /// <summary>
        /// 设置KV
        /// </summary>
        /// <param name="kv"></param>
        /// <returns></returns>
        public static bool Put(KVPair kv)
        {
            using (var client = new ConsulClient())
            {
                return client.KV.Put(kv).Result.Response;
            }
        }

        /// <summary>
        /// 删除对应Key
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static bool Delete(string key)
        {
            using (var client = new ConsulClient())
            {
                return client.KV.Delete(key).Result.Response;
            }
        }

        /// <summary>
        /// 删除对应前缀的所有Key
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static bool DeleteTree(string prefix)
        {
            using (var client = new ConsulClient())
            {
                return client.KV.DeleteTree(prefix).Result.Response;
            }
        }

        /// <summary>
        /// 获取对应前缀的所有Key
        /// </summary>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public static string[] Keys(string prefix)
        {
            using (var client = new ConsulClient())
            {
                return client.KV.Keys(prefix).Result.Response;
            }
        }

        /// <summary>
        /// 获取对应前缀的所有Key
        /// </summary>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public static string[] Keys(string prefix, string separator)
        {
            using (var client = new ConsulClient())
            {
                return client.KV.Keys(prefix, separator).Result.Response;
            }
        }

        /// <summary>
        /// 获取对应前缀的所有KVPair
        /// </summary>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public static KVPair[] List(string prefix)
        {
            using (var client = new ConsulClient())
            {
                return client.KV.List(prefix).Result.Response;
            }
        }

        /// <summary>
        /// 创建Session
        /// </summary>
        /// <returns></returns>
        public static string CreateSession()
        {
            using (var client = new ConsulClient())
            {
                return client.Session.Create().Result.Response;
            }
        }

        /// <summary>
        /// 移除Session
        /// </summary>
        /// <returns></returns>
        public static bool RemoveSession(string sessionId)
        {
            using (var client = new ConsulClient())
            {
                return client.Session.Destroy(sessionId).Result.Response;
            }
        }

        /// <summary>
        /// Check ModifyIndex And Set
        /// </summary>
        /// <returns></returns>
        public static bool CAS(KVPair kv)
        {
            using (var client = new ConsulClient())
            {
                return client.KV.CAS(kv).Result.Response;
            }
        }
    }
}
