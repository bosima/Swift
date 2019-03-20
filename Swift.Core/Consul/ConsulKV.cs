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
    /// Consul KV相关
    /// </summary>
    public static class ConsulKV
    {
        private static ConsulClient client = new ConsulClient();

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
        /// 阻塞获取对应Key的值
        /// </summary>
        /// <returns>The get.</returns>
        /// <param name="key">Key.</param>
        /// <param name="waitTime">Wait time.</param>
        public static KVPair BlockGet(string key, TimeSpan waitTime, ulong waitIndex)
        {
            return Retry(() =>
            {
                return client.KV.Get(key, new QueryOptions()
                {
                    WaitTime = waitTime,
                    WaitIndex = waitIndex
                }).Result.Response;
            }, 1);
        }

        /// <summary>
        /// 获取对应Key的字符串值
        /// </summary>
        /// <param name="kv"></param>
        /// <returns></returns>
        public static bool Acquire(KVPair kv)
        {
            return Retry(() =>
            {
                return client.KV.Acquire(kv).Result.Response;
            }, 1);
        }

        /// <summary>
        /// 释放对应Key
        /// </summary>
        /// <param name="kv"></param>
        /// <returns></returns>
        public static bool Release(KVPair kv)
        {
            return Retry(() =>
            {
                return client.KV.Release(kv).Result.Response;
            }, 1);
        }

        /// <summary>
        /// 获取对应Key的字符串值
        /// </summary>
        /// <returns>The value string.</returns>
        /// <param name="key">Key.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public static string GetValueString(string key, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Retry(() =>
            {
                var kvPair = client.KV.Get(key, cancellationToken).Result;
                if (kvPair.Response != null && kvPair.Response.Value != null)
                {
                    return Encoding.UTF8.GetString(kvPair.Response.Value, 0, kvPair.Response.Value.Length);
                }
                return string.Empty;
            }, 2);
        }

        /// <summary>
        /// 获取对应Key的字符串值
        /// </summary>
        /// <returns>The get.</returns>
        /// <param name="key">Key.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public static KVPair Get(string key, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Retry(() =>
            {
                return client.KV.Get(key, cancellationToken).Result.Response;
            }, 2);
        }

        /// <summary>
        /// 设置KV
        /// </summary>
        /// <param name="kv"></param>
        /// <returns></returns>
        public static bool Put(KVPair kv)
        {
            return Retry(() =>
            {
                return client.KV.Put(kv).Result.Response;
            }, 2);
        }

        /// <summary>
        /// 删除对应Key
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static bool Delete(string key)
        {
            return Retry(() =>
            {
                return client.KV.Delete(key).Result.Response;
            }, 2);
        }

        /// <summary>
        /// 删除对应前缀的所有Key
        /// </summary>
        /// <returns><c>true</c>, if tree was deleted, <c>false</c> otherwise.</returns>
        /// <param name="prefix">Prefix.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public static bool DeleteTree(string prefix, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Retry(() =>
            {
                return client.KV.DeleteTree(prefix, cancellationToken).Result.Response;
            }, 2);
        }

        /// <summary>
        /// 获取对应前缀的所有Key
        /// </summary>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public static string[] Keys(string prefix, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Retry(() =>
            {
                return client.KV.Keys(prefix, cancellationToken).Result.Response;
            }, 2);
        }

        /// <summary>
        /// 获取对应前缀的所有Key
        /// </summary>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public static string[] Keys(string prefix, string separator)
        {
            return Retry(() =>
            {
                return client.KV.Keys(prefix, separator).Result.Response;
            }, 2);
        }

        /// <summary>
        /// 获取对应前缀的所有KVPair
        /// </summary>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public static KVPair[] List(string prefix)
        {
            return Retry(() =>
            {
                return client.KV.List(prefix).Result.Response;
            }, 2);
        }

        /// <summary>
        /// 创建Session
        /// </summary>
        /// <returns>The session.</returns>
        /// <param name="checkId">Check identifier.</param>
        /// <param name="lockDelay">Lock delay.</param>
        public static string CreateSession(string checkId, int lockDelay = 15)
        {
            return Retry(() =>
            {
                return client.Session.Create(new SessionEntry()
                {
                    Checks = new List<string> { checkId },
                    LockDelay = new TimeSpan(0, 0, lockDelay),

                }).Result.Response;
            }, 2);
        }

        /// <summary>
        /// 移除Session
        /// </summary>
        /// <returns></returns>
        public static bool RemoveSession(string sessionId)
        {
            return Retry(() =>
            {
                return client.Session.Destroy(sessionId).Result.Response;
            }, 2);
        }

        /// <summary>
        /// Check ModifyIndex And Set
        /// </summary>
        /// <returns></returns>
        public static bool CAS(KVPair kv, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Retry(() =>
            {
                return client.KV.CAS(kv, cancellationToken).Result.Response;
            }, 2);
        }

        private static void Retry(Action action, int retryTimes)
        {
            int i = retryTimes;
            while (i > 0)
            {
                try
                {
                    action();
                }
                catch (AggregateException ex)
                {
                    LogWriter.Write("执行ConsulKV操作异常。", ex);

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
                    LogWriter.Write("执行ConsulKV操作异常。", ex);

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
