using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Swift.Core
{
    /// <summary>
    /// 作业包装类：用于在执行各种作业时作业类型多变而导致表达困难的问题，可方便转换为JobBase具体子类的实例
    /// </summary>
    public class JobWrapper : JobBase
    {
        public JobWrapper()
        {
        }

        public JobWrapper(JobBase job)
        {
            if (job != null)
            {
                Id = job.Id;
                Name = job.Name;
                FileName = job.FileName;
                JobClassName = job.JobClassName;
                Status = job.Status;
            }
        }

        /// <summary>
        /// 分割作业为不同的任务
        /// </summary>
        /// <returns></returns>
        public override JobTask[] Split()
        {
            throw new NotImplementedException("这个方法不会被实现，应继承JobBase实现自己的Split");
        }

        /// <summary>
        /// 处理具体的任务
        /// </summary>
        /// <param name="task"></param>
        /// <returns></returns>
        public override string ExecuteTask(JobTask task)
        {
            throw new NotImplementedException("这个方法不会被实现，应继承JobBase实现自己的Process");
        }

        /// <summary>
        /// 汇集各个任务的处理结果
        /// </summary>
        /// <returns></returns>
        public override string Collect(IEnumerable<JobTask> tasks)
        {
            throw new NotImplementedException("这个方法不会被实现，应继承JobBase实现自己的Collect");
        }

        /// <summary>
        /// 转换为JobBase的某个具体子类的实例
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public T ConvertTo<T>() where T : JobBase
        {
            var t = System.Activator.CreateInstance<T>();
            t.FileName = this.FileName;
            t.Id = this.Id;
            t.Name = this.Name;
            t.JobClassName = this.JobClassName;
            t.Status = this.Status;
            t.TaskPlan = this.TaskPlan;
            t.CreateTime = this.CreateTime;
            t.ModifyIndex = this.ModifyIndex;
            return t;
        }
    }
}
