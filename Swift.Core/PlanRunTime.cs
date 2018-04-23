using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Swift.Core
{
    /// <summary>
    /// 计划运行时间
    /// </summary>
    public class PlanRunTime
    {
        public PlanRunTime(string stringValue)
        {
            StringValue = stringValue;

            /// HH:mm 每天定时运行
            /// ddd HH:mm 每周定时运行
            /// MM-dd HH:mm 每月定时运行
            /// yyyy-MM-dd HH:mm 定时运行一次

            if (!stringValue.Contains(" "))
            {
                PlanType = 1;

                var hmArray = stringValue.Split(':');
                Hour = int.Parse(hmArray[0]);
                Minute = int.Parse(hmArray[1]);
            }
            else
            {

                if (!stringValue.Contains("-"))
                {
                    PlanType = 2;

                    var wArray = stringValue.Split(' ');
                    WeekDay = GetWeekDay(wArray[0]);
                    if (!WeekDay.HasValue)
                    {
                        throw new ArgumentOutOfRangeException("运行时间计划格式无效");
                    }

                    var hmArray = wArray[1].Split(':');
                    Hour = int.Parse(hmArray[0]);
                    Minute = int.Parse(hmArray[1]);
                }
                else if (stringValue.IndexOf("-") == stringValue.LastIndexOf("-"))
                {
                    PlanType = 3;

                    var mArray = stringValue.Split(' ');

                    var mdArray = mArray[0].Split('-');
                    Month = int.Parse(mdArray[0]);
                    Day = int.Parse(mdArray[1]);

                    var hmArray = mArray[1].Split(':');
                    Hour = int.Parse(hmArray[0]);
                    Minute = int.Parse(hmArray[1]);
                }
                else
                {
                    PlanType = 4;

                    var mArray = stringValue.Split(' ');

                    var mdArray = mArray[0].Split('-');
                    Year = int.Parse(mdArray[0]);
                    Month = int.Parse(mdArray[1]);
                    Day = int.Parse(mdArray[2]);

                    var hmArray = mArray[1].Split(':');
                    Hour = int.Parse(hmArray[0]);
                    Minute = int.Parse(hmArray[1]);
                }
            }
        }

        /// <summary>
        /// 运行时间计划的字符串值
        /// </summary>
        public string StringValue { get; private set; }

        /// <summary>
        /// 计划运行时间的年份
        /// </summary>
        public int? Year { get; private set; }

        /// <summary>
        /// 计划运行时间的月份
        /// </summary>
        public int? Month { get; private set; }

        /// <summary>
        /// 计划运行时间的日期
        /// </summary>
        public int? Day { get; private set; }

        /// <summary>
        /// 计划运行时间的星期
        /// </summary>
        public DayOfWeek? WeekDay { get; private set; }

        /// <summary>
        /// 计划运行时间的小时
        /// </summary>
        public int Hour { get; private set; }

        /// <summary>
        /// 计划运行时间的分钟
        /// </summary>
        public int Minute { get; private set; }

        /// <summary>
        /// 计划类型:
        /// 1 每天定时运行
        /// 2 每周定时运行
        /// 3 每月定时运行
        /// 4 定时运行一次
        /// </summary>
        public int PlanType { get; private set; }

        /// <summary>
        /// 判断当前是否到达运行时间
        /// </summary>
        /// <returns></returns>
        public bool CheckIsTime()
        {
            DateTime now = DateTime.Now;

            if (PlanType == 1)
            {
                if (now.Hour == Hour && now.Minute == Minute)
                {
                    return true;
                }
            }

            if (PlanType == 2)
            {
                if (now.DayOfWeek == WeekDay
                    && now.Hour == Hour && now.Minute == Minute)
                {
                    return true;
                }
            }

            if (PlanType == 3)
            {
                if (now.Month == Month && now.Day == Day
                    && now.Hour == Hour && now.Minute == Minute)
                {
                    return true;
                }
            }

            if (PlanType == 4)
            {
                if (now.Year == Year
                    && now.Month == Month && now.Day == Day
                    && now.Hour == Hour && now.Minute == Minute)
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// 根据星期简称获取星期序号：1至7
        /// </summary>
        /// <param name="weekSimpleName"></param>
        /// <returns></returns>
        private DayOfWeek? GetWeekDay(string weekSimpleName)
        {
            switch (weekSimpleName)
            {
                case "Mon":
                    return DayOfWeek.Monday;
                case "Tues":
                    return DayOfWeek.Tuesday;
                case "Wed":
                    return DayOfWeek.Wednesday;
                case "Thur":
                    return DayOfWeek.Thursday;
                case "Fri":
                    return DayOfWeek.Friday;
                case "Sat":
                    return DayOfWeek.Saturday;
                case "Sun":
                    return DayOfWeek.Sunday;
                default:
                    return null;
            }
        }
    }
}
