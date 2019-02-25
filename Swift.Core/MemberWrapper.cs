using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Swift.Core
{
    /// <summary>
    /// 成员包装类：用于在反序列化时
    /// </summary>
    public class MemberWrapper : Member
    {
        public MemberWrapper()
        {
        }

        public MemberWrapper(Member member)
        {
            if (member != null)
            {
                Id = this.Id;
                FirstRegisterTime = this.FirstRegisterTime;
                OfflineTime = this.OfflineTime;
                OnlineTime = this.OnlineTime;
                Role = this.Role;
                Status = this.Status;
            }
        }

        /// <summary>
        /// 开始处理
        /// </summary>
        protected override void Start()
        {
        }

        /// <summary>
        /// 停止处理
        /// </summary>
        protected override void Stop()
        {
        }

        /// <summary>
        /// 转换为Member的某个具体子类的实例
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public T ConvertTo<T>() where T : Member
        {
            var t = System.Activator.CreateInstance<T>();
            t.Id = this.Id;
            t.FirstRegisterTime = this.FirstRegisterTime;
            t.OfflineTime = this.OfflineTime;
            t.OnlineTime = this.OnlineTime;
            t.Role = this.Role;
            t.Status = this.Status;
            return t;
        }

        /// <summary>
        /// 转换为基类
        /// </summary>
        /// <returns></returns>
        public Member ConvertToBase()
        {
            return this;
        }
    }
}
