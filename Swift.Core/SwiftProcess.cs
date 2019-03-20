using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Newtonsoft.Json;
using Swift.Core.Log;

namespace Swift.Core
{
    /// <summary>
    /// Swift进程
    /// </summary>
    public class SwiftProcess
    {
        private readonly JobBase _job;
        private readonly JobTask _jobTask;
        private readonly int _timeout;
        private readonly string _businessId;
        private SwiftProcessEventActions _eventActions;
        private Process _process;
        private readonly string _method;
        private readonly string _filePath = string.Empty;

        public SwiftProcess(string method, JobBase job, SwiftProcessEventActions eventActions)
        {
            _method = method;
            switch (_method)
            {
                case "SplitJob":
                    _timeout = job.JobSplitTimeout;
                    break;
                case "CollectTaskResult":
                    _timeout = job.TaskResultCollectTimeout;
                    break;
                default:
                    _timeout = 0;
                    break;
            }

            _job = job;
            _businessId = job.BusinessId;
            _eventActions = eventActions;
            _process = new Process();
            _filePath = SwiftConfiguration.GetSwiftProcessPath(_method, _businessId);
            _job.RelateProcess(method, this);
        }

        public SwiftProcess(string method, JobBase job, Process process)
        {
            _method = method;
            switch (_method)
            {
                case "SplitJob":
                    _timeout = job.JobSplitTimeout;
                    break;
                case "CollectTaskResult":
                    _timeout = job.TaskResultCollectTimeout;
                    break;
                default:
                    _timeout = 0;
                    break;
            }

            _job = job;
            _businessId = job.BusinessId;
            _eventActions = null;
            _process = process;
            _jobTask.Process = this;
            _filePath = SwiftConfiguration.GetSwiftProcessPath(_method, _businessId);
            _job.RelateProcess(method, this);
        }

        public SwiftProcess(string method, JobTask task, SwiftProcessEventActions eventActions)
        {
            _method = method;
            _jobTask = task;
            _timeout = _jobTask.ExecuteTimeout;
            _job = task.Job;
            _businessId = task.BusinessId;
            _eventActions = eventActions;
            _process = new Process();
            _jobTask.Process = this;
            _filePath = SwiftConfiguration.GetSwiftProcessPath(_method, _businessId);
        }

        public SwiftProcess(string method, JobTask task, Process process)
        {
            _method = method;
            _jobTask = task;
            _timeout = _jobTask.ExecuteTimeout;
            _job = task.Job;
            _businessId = task.BusinessId;
            _eventActions = null;
            _process = process;
            _jobTask.Process = this;
            _filePath = SwiftConfiguration.GetSwiftProcessPath(_method, _businessId);
        }

        /// <summary>
        /// 根据进程Id获取实例
        /// </summary>
        /// <returns>The get by identifier.</returns>
        /// <param name="processId">Process identifier.</param>
        /// <param name="task">Task.</param>
        /// <param name="process">Process.</param>
        public static bool TryGetById(int processId, JobTask task, out SwiftProcess process)
        {
            process = null;
            Process osProcess;
            try
            {
                osProcess = Process.GetProcessById(processId);
            }
            catch (Exception ex)
            {
                LogWriter.Write(string.Format("根据进程Id查找进程失败:{0},{1}", task.BusinessId, processId), ex, LogLevel.Info);
                return false;
            }

            LogWriter.Write("已经根据进程Id找到进程：" + processId);

            if (osProcess != null)
            {
                if (CheckTaskAndProcessMatch(task, osProcess, out SwiftProcessCommandLine commandLine))
                {
                    process = new SwiftProcess("ExecuteTask", task, osProcess);
                    LogWriter.Write(string.Format("已创建出SwiftProcess实例：{0},{1}", processId, task.BusinessId));
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// 根据进程Id获取实例
        /// </summary>
        /// <returns>The get by identifier.</returns>
        /// <param name="processId">Process identifier.</param>
        /// <param name="job">Task.</param>
        /// <param name="process">Process.</param>
        public static bool TryGetById(int processId, JobBase job, string method, out SwiftProcess process)
        {
            process = null;
            Process osProcess;
            try
            {
                osProcess = Process.GetProcessById(processId);
            }
            catch (Exception ex)
            {
                LogWriter.Write(string.Format("根据进程Id查找进程失败:{0},{1}", job.BusinessId, processId), ex, LogLevel.Info);
                return false;
            }

            LogWriter.Write("已经根据进程Id找到进程：" + processId);

            if (osProcess != null)
            {
                if (CheckJobAndProcessMatch(processId, job.Name, job.Id, method))
                {
                    process = new SwiftProcess(method, job, osProcess);
                    LogWriter.Write(string.Format("已创建出SwiftProcess实例：{0},{1}", processId, job.BusinessId));
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// 获取所有本地进程
        /// </summary>
        public static List<string[]> GetAllLocalProcess()
        {
            List<string[]> processList = new List<string[]>();
            var processDirPath = SwiftConfiguration.AllSwiftProcessRootPath;

            if (!Directory.Exists(processDirPath))
            {
                return processList;
            }

            var processFiles = Directory.GetFiles(processDirPath);
            if (processFiles.Length > 0)
            {
                foreach (var processPath in processFiles)
                {
                    try
                    {
                        var processId = File.ReadAllText(processPath);
                        var fileName = processPath.Substring(processPath.LastIndexOf(Path.DirectorySeparatorChar) + 1);
                        LogWriter.Write("GetAllLocalProcess->fileName:" + fileName, LogLevel.Trace);
                        var processTypeIndex = fileName.IndexOf('-');
                        var processType = fileName.Substring(0, processTypeIndex);
                        var businessId = fileName.Substring(processTypeIndex + 1, fileName.LastIndexOf('.') - processTypeIndex - 1);
                        var businessIdArray = businessId.Split('_');
                        var jobName = businessIdArray[0];
                        var jobId = businessIdArray[1];

                        switch (processType)
                        {
                            case "ExecuteTask":
                                var taskId = businessIdArray[2];
                                processList.Add(new string[] { "ExecuteTask", processId, jobName, jobId, taskId });
                                break;
                            case "SplitJob":
                                processList.Add(new string[] { "SplitJob", processId, jobName, jobId });
                                break;
                            case "CollectTaskResult":
                                processList.Add(new string[] { "CollectTaskResult", processId, jobName, jobId });
                                break;
                            default:
                                throw new NotSupportedException("find not supported process type: " + processType);
                        }
                    }
                    catch (Exception ex)
                    {
                        LogWriter.Write("read process file exception", ex);
                    }
                }
            }

            return processList;
        }

        /// <summary>
        /// 进程对应的业务Id
        /// </summary>
        /// <value>The business identifier.</value>
        public string BusinessId
        {
            get
            {
                return _businessId;
            }
        }

        /// <summary>
        /// 进程对应的Id文件路径
        /// </summary>
        /// <value>The business identifier.</value>
        public string IdFilePath
        {
            get
            {
                return _filePath;
            }
        }

        /// <summary>
        /// 机器进程Id
        /// </summary>
        /// <value>The process identifier.</value>
        public int Id
        {
            get
            {
                return _process.Id;
            }
        }

        /// <summary>
        /// 进程是否存在
        /// </summary>
        /// <value><c>true</c> if is exist; otherwise, <c>false</c>.</value>
        public bool IsExist
        {
            get
            {
                return _process == null;
            }
        }

        /// <summary>
        /// 关连的进程是否已退出
        /// </summary>
        /// <value>The process identifier.</value>
        public bool HasExited
        {
            get
            {
                if (_process != null)
                {
                    try
                    {
                        return _process.HasExited;
                    }
                    catch (Exception ex)
                    {
                        LogWriter.Write("获取进程HasExited属性异常", ex, LogLevel.Info);
                        return true;
                    }
                }
                return true;
            }
        }

        /// <summary>
        /// Gets the exit code.
        /// </summary>
        /// <value>The exit code.</value>
        public int ExitCode
        {
            get
            {
                return _process.ExitCode;
            }
        }

        /// <summary>
        /// 进程实际启动时间
        /// </summary>
        /// <value>The start time.</value>
        public DateTime StartTime
        {
            get
            {
                return _process.StartTime;
            }
        }

        /// <summary>
        /// 向进程发出关闭命令
        /// </summary>
        public void Kill()
        {
            _process.Kill();
        }

        /// <summary>
        /// 等待进程退出
        /// </summary>
        public void WaitForExit()
        {
            _process.WaitForExit();
        }

        /// <summary>
        /// Kills the abandoned task process.
        /// </summary>
        /// <param name="processId">Process identifier.</param>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        /// <param name="taskId">Task identifier.</param>
        public static void KillAbandonedTaskProcess(int processId, string jobName, string jobId, int taskId)
        {
            var businessId = JobTask.FormatBusinessId(jobName, jobId, taskId);

            Process osProcess = null;
            try
            {
                osProcess = Process.GetProcessById(processId);
            }
            catch (Exception ex)
            {
                LogWriter.Write(string.Format("根据进程Id查找进程异常，进程可能已经关闭了:{0},{1}", businessId, processId), ex, LogLevel.Info);
            }

            bool canDeleteProcessFile = true;
            if (osProcess != null)
            {
                if (SwiftProcess.CheckTaskAndProcessMatch(osProcess, jobName, jobId, taskId))
                {
                    try
                    {
                        osProcess.Kill();
                        osProcess.WaitForExit();
                        LogWriter.Write(string.Format("已关闭任务废弃的进程:{0},{1}", businessId, processId), LogLevel.Info);
                    }
                    catch (Exception ex)
                    {
                        canDeleteProcessFile = false;
                        LogWriter.Write(string.Format("关闭任务废弃的进程异常:{0},{1}", businessId, processId), ex, LogLevel.Error);
                    }
                }
            }

            if (canDeleteProcessFile)
            {
                var processPath = SwiftConfiguration.GetSwiftProcessPath("ExecuteTask", JobTask.FormatBusinessId(jobName, jobId, taskId));
                try
                {
                    File.Delete(processPath);
                    LogWriter.Write(string.Format("进程文件已删除：{0}", processPath), LogLevel.Info);
                }
                catch (Exception ex)
                {
                    LogWriter.Write(string.Format("删除任务废弃的进程文件异常:{0},{1}", businessId, processId), ex, LogLevel.Error);
                }
            }
        }

        /// <summary>
        /// Kills the abandoned job split process.
        /// </summary>
        /// <param name="processId">Process identifier.</param>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        public static void KillAbandonedJobSplitProcess(int processId, string jobName, string jobId)
        {
            var businessId = JobBase.FormatBusinessId(jobName, jobId);

            Process osProcess = null;
            try
            {
                osProcess = Process.GetProcessById(processId);
            }
            catch (Exception ex)
            {
                LogWriter.Write(string.Format("根据进程Id查找进程异常，进程可能已经关闭了:{0},{1}", businessId, processId), ex, LogLevel.Info);
            }

            bool canDeleteProcessFile = true;
            if (osProcess != null)
            {
                if (SwiftProcess.CheckJobAndProcessMatch(osProcess, jobName, jobId, "SplitJob"))
                {
                    try
                    {
                        osProcess.Kill();
                        osProcess.WaitForExit();
                        LogWriter.Write(string.Format("已关闭废弃的作业分割进程:{0},{1}", businessId, processId), LogLevel.Info);
                    }
                    catch (Exception ex)
                    {
                        canDeleteProcessFile = false;
                        LogWriter.Write(string.Format("关闭废弃的作业分割进程异常:{0},{1}", businessId, processId), ex, LogLevel.Error);
                    }
                }
            }

            if (canDeleteProcessFile)
            {
                var processPath = SwiftConfiguration.GetSwiftProcessPath("SplitJob", JobBase.FormatBusinessId(jobName, jobId));
                try
                {
                    File.Delete(processPath);
                    LogWriter.Write(string.Format("进程文件已删除：{0}", processPath), LogLevel.Info);
                }
                catch (Exception ex)
                {
                    LogWriter.Write(string.Format("删除废弃的作业分割进程文件异常:{0},{1}", businessId, processId), ex, LogLevel.Error);
                }
            }
        }

        /// <summary>
        /// Kills the abandoned collect task result process.
        /// </summary>
        /// <param name="processId">Process identifier.</param>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        public static void KillAbandonedCollectTaskResultProcess(int processId, string jobName, string jobId)
        {
            var businessId = JobBase.FormatBusinessId(jobName, jobId);

            Process osProcess = null;
            try
            {
                osProcess = Process.GetProcessById(processId);
            }
            catch (Exception ex)
            {
                LogWriter.Write(string.Format("根据进程Id查找进程异常，进程可能已经关闭了:{0},{1}", businessId, processId), ex, LogLevel.Info);
            }

            bool canDeleteProcessFile = true;
            if (osProcess != null)
            {
                if (SwiftProcess.CheckJobAndProcessMatch(osProcess, jobName, jobId, "CollectTaskResult"))
                {
                    try
                    {
                        osProcess.Kill();
                        osProcess.WaitForExit();
                        LogWriter.Write(string.Format("已关闭废弃的任务合并进程:{0},{1}", businessId, processId), LogLevel.Info);
                    }
                    catch (Exception ex)
                    {
                        canDeleteProcessFile = false;
                        LogWriter.Write(string.Format("关闭废弃的任务合并进程异常:{0},{1}", businessId, processId), ex, LogLevel.Error);
                    }
                }
            }

            if (canDeleteProcessFile)
            {
                var processPath = SwiftConfiguration.GetSwiftProcessPath("CollectTaskResult", JobBase.FormatBusinessId(jobName, jobId));
                try
                {
                    File.Delete(processPath);
                    LogWriter.Write(string.Format("进程文件已删除：{0}", processPath), LogLevel.Info);
                }
                catch (Exception ex)
                {
                    LogWriter.Write(string.Format("删除废弃的任务合并进程文件异常:{0},{1}", businessId, processId), ex, LogLevel.Error);
                }
            }
        }

        /// <summary>
        /// 检查任务和进程是否匹配
        /// </summary>
        /// <returns><c>true</c>, if task and process match was checked, <c>false</c> otherwise.</returns>
        /// <param name="processId">Process identifier.</param>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        /// <param name="taskId">Task identifier.</param>
        public static bool CheckTaskAndProcessMatch(int processId, string jobName, string jobId, int taskId)
        {
            var businessId = JobTask.FormatBusinessId(jobName, jobId, taskId);

            Process osProcess = null;
            try
            {
                osProcess = Process.GetProcessById(processId);
            }
            catch (Exception ex)
            {
                LogWriter.Write(string.Format("根据进程Id查找进程失败:{0},{1}", businessId, processId), ex, LogLevel.Info);
            }

            if (osProcess == null)
            {
                return false;
            }

            return CheckTaskAndProcessMatch(osProcess, jobName, jobId, taskId);
        }

        /// <summary>
        /// 检查任务和进程是否匹配
        /// </summary>
        /// <returns><c>true</c>, if task and process match was checked, <c>false</c> otherwise.</returns>
        /// <param name="osProcess">Process.</param>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        /// <param name="taskId">Task identifier.</param>
        public static bool CheckTaskAndProcessMatch(Process osProcess, string jobName, string jobId, int taskId)
        {
            SwiftProcessCommandLine commandLine = null;
            try
            {
                commandLine = SwiftProcessCommandLine.Get(osProcess, EnumExecutableFileType.DirectExe | EnumExecutableFileType.DotNet);
            }
            catch (Exception ex)
            {
                LogWriter.Write(string.Format("获取并格式化进程命令行异常:{0}", osProcess.Id), ex, LogLevel.Info);
            }

            if (commandLine == null)
            {
                return false;
            }

            if (!commandLine.Paras.TryGetValue("-jn", out string cJobName))
            {
                return false;
            }

            if (!commandLine.Paras.TryGetValue("-jr", out string cJobRecordId))
            {
                return false;
            }

            if (!commandLine.Paras.TryGetValue("-t", out string cTaskId))
            {
                return false;
            }

            if (jobName == cJobName && jobId == cJobRecordId && taskId == int.Parse(cTaskId))
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// 检查作业和进程是否匹配
        /// </summary>
        /// <returns><c>true</c>, if job and process match was checked, <c>false</c> otherwise.</returns>
        /// <param name="processId">Process identifier.</param>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        /// <param name="method">Method.</param>
        public static bool CheckJobAndProcessMatch(int processId, string jobName, string jobId, string method)
        {
            var businessId = JobBase.FormatBusinessId(jobName, jobId);

            Process osProcess = null;
            try
            {
                osProcess = Process.GetProcessById(processId);
            }
            catch (Exception ex)
            {
                LogWriter.Write(string.Format("根据进程Id查找进程失败:{0},{1}", businessId, processId), ex, LogLevel.Info);
            }

            if (osProcess == null)
            {
                return false;
            }

            return CheckJobAndProcessMatch(osProcess, jobName, jobId, method);
        }

        /// <summary>
        /// 检查作业和进程是否匹配
        /// </summary>
        /// <returns><c>true</c>, if job and process match was checked, <c>false</c> otherwise.</returns>
        /// <param name="osProcess">Os process.</param>
        /// <param name="jobName">Job name.</param>
        /// <param name="jobId">Job identifier.</param>
        /// <param name="method">Method.</param>
        public static bool CheckJobAndProcessMatch(Process osProcess, string jobName, string jobId, string method)
        {
            SwiftProcessCommandLine commandLine = null;
            try
            {
                commandLine = SwiftProcessCommandLine.Get(osProcess, EnumExecutableFileType.DirectExe | EnumExecutableFileType.DotNet);
            }
            catch (Exception ex)
            {
                LogWriter.Write(string.Format("获取并格式化进程命令行异常:{0}", osProcess.Id), ex, LogLevel.Info);
            }

            if (commandLine == null)
            {
                return false;
            }

            if (!commandLine.Paras.TryGetValue("-jn", out string cJobName))
            {
                return false;
            }

            if (!commandLine.Paras.TryGetValue("-jr", out string cJobRecordId))
            {
                return false;
            }

            if (method == "SplitJob" && !commandLine.Paras.ContainsKey("-d"))
            {
                return false;
            }

            if (method == "CollectTaskResult" && !commandLine.Paras.ContainsKey("-m"))
            {
                return false;
            }

            if (jobName == cJobName && jobId == cJobRecordId)
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// 执行进程
        /// </summary>
        /// <param name="method">Method.</param>
        /// <param name="workingDirectory">Working directory.</param>
        /// <param name="fileName">File name.</param>
        /// <param name="arguments">Arguments.</param>
        private void Execute(string method, string workingDirectory, string fileName, string arguments)
        {
            try
            {
                _process.StartInfo.WorkingDirectory = workingDirectory;
                _process.StartInfo.FileName = fileName;
                _process.StartInfo.Arguments = arguments;
                _process.StartInfo.UseShellExecute = false;
                _process.StartInfo.RedirectStandardOutput = true;
                _process.StartInfo.RedirectStandardError = true;
                _process.StartInfo.CreateNoWindow = true;
                _process.EnableRaisingEvents = true;

                _process.OutputDataReceived += (s, e) =>
                {
                    if (_eventActions != null)
                    {
                        _eventActions.OutputAction?.Invoke(this, e);
                    }
                };

                _process.ErrorDataReceived += (s, e) =>
                {
                    if (_eventActions != null)
                    {
                        _eventActions.ErrorAction?.Invoke(this, e);
                    }
                };

                _process.Exited += (s, e) =>
                {
                    if (_eventActions != null)
                    {
                        _eventActions.ExitAction?.Invoke(this, e);
                    }
                };

                _process.Start();
                _process.BeginOutputReadLine();
                _process.BeginErrorReadLine();

                LogWriter.Write(string.Format("[{0}][{1}] process has start", method, _businessId), Log.LogLevel.Info);
                if (_eventActions != null)
                {
                    _eventActions.StartedAction?.Invoke(this, EventArgs.Empty);
                }

                if (_timeout > 0)
                {
                    var processWaitTime = _timeout * 60 * 1000;
                    LogWriter.Write(string.Format("[{0}][{1}] process will wait time: {2}", method, _businessId, processWaitTime), Log.LogLevel.Info);

                    if (!_process.WaitForExit(processWaitTime))
                    {
                        if (_eventActions != null)
                        {
                            _eventActions.TimeoutAction?.Invoke(this, EventArgs.Empty);
                        }

                        LogWriter.Write(string.Format("[{0}][{1}] process timeout", method, _businessId), Log.LogLevel.Warn);

                        try
                        {
                            _process.Kill();
                            LogWriter.Write(string.Format("[{0}][{1}] has send kill command", method, _businessId), Log.LogLevel.Warn);
                        }
                        catch (Exception ex)
                        {
                            LogWriter.Write(string.Format("[{0}][{1}] kill timeouted process but exception", method, _businessId), ex);
                        }
                        _process.WaitForExit();
                    }
                }
                else
                {
                    _process.WaitForExit();
                }
                _process.Close();
                LogWriter.Write(string.Format("[{0}][{1}] process close", method, _businessId), Log.LogLevel.Info);
            }
            catch (Exception ex)
            {
                LogWriter.Write(string.Format("[{0}][{1}] process execute exception", method, _businessId), ex);
            }
        }

        /// <summary>
        /// 分割作业
        /// </summary>
        public void SplitJob(CancellationToken cancellationToken = default(CancellationToken))
        {
            // 当前作业执行命令
            var fileName = _job.FileName;
            var paras = string.Format("-d -jr {0} -jn {1}", _job.Id, _job.Name);

            var commandParas = FormatProcessCommandParas(fileName, paras);
            fileName = commandParas.Item1;
            paras = commandParas.Item2;

            var commandLine = string.Format("{0}:{1}:{2}", _job.CurrentJobSpacePath, fileName, paras);

            LogWriter.Write(string.Format("SwiftProcess->SplitJob Command: {0}", commandLine), Log.LogLevel.Debug);

            using (var ctsTokenRegistion = cancellationToken.Register(Kill))
            {
                Execute("SplitJob", _job.CurrentJobSpacePath, fileName, paras);
            }
        }

        /// <summary>
        /// 执行任务
        /// </summary>
        public void ExecuteTask(CancellationToken cancellationToken = default(CancellationToken))
        {
            // 当前作业执行命令
            var fileName = _job.FileName;
            var paras = string.Format(" -p -t {0} -jr {1} -jn {2}", _jobTask.Id, _job.Id, _job.Name);

            var commandParas = FormatProcessCommandParas(fileName, paras);
            LogWriter.Write(string.Format("已格式化命令行参数"), Log.LogLevel.Trace);
            fileName = commandParas.Item1;
            paras = commandParas.Item2;

            var commandLine = string.Format("{0}:{1}:{2}", _job.CurrentJobSpacePath, fileName, paras);

            LogWriter.Write(string.Format("SwiftProcess->ExecuteTask Command: {0}", commandLine), Log.LogLevel.Debug);

            using (var ctsTokenRegistion = cancellationToken.Register(Kill))
            {
                Execute("ExecuteTask", _job.CurrentJobSpacePath, fileName, paras);
            }
        }

        /// <summary>
        /// 合并任务结果
        /// </summary>
        public void CollectTaskResult(CancellationToken cancellationToken = default(CancellationToken))
        {
            // 当前作业执行命令
            var fileName = _job.FileName;
            var paras = string.Format(" -m -jr {0} -jn {1}", _job.Id, _job.Name);

            var commandParas = FormatProcessCommandParas(fileName, paras);
            LogWriter.Write(string.Format("已格式化命令行参数"), Log.LogLevel.Trace);
            fileName = commandParas.Item1;
            paras = commandParas.Item2;

            var commandLine = string.Format("{0}:{1}:{2}", _job.CurrentJobSpacePath, fileName, paras);

            LogWriter.Write(string.Format("SwiftProcess->CollectTaskResult Command: {0}", commandLine), Log.LogLevel.Debug);

            using (var ctsTokenRegistion = cancellationToken.Register(Kill))
            {
                Execute("CollectTaskResult", _job.CurrentJobSpacePath, fileName, paras);
            }
        }

        /// <summary>
        /// 格式化启动进程命令行参数
        /// </summary>
        /// <returns>The process command paras.</returns>
        /// <param name="fileName">File name.</param>
        /// <param name="paras">Paras.</param>
        private Tuple<string, string> FormatProcessCommandParas(string fileName, string paras)
        {
            var processFileName = string.Empty;
            var processParas = string.Empty;

            if (_job.ExeType == "exe") // windows exe
            {
                processFileName = fileName;
                processParas = paras;
            }
            else if (_job.ExeType == "dotnet") // dotnet core
            {
                processFileName = "dotnet";
                processParas = " " + fileName + " " + paras;
            }
            else
            {
                throw new NotSupportedException("not supported exe type: " + _job.ExeType);
            }

            return new Tuple<string, string>(processFileName, processParas);
        }

        /// <summary>
        /// 检查进程和任务是否匹配
        /// </summary>
        /// <returns>The task and process match.</returns>
        /// <param name="task">Task.</param>
        /// <param name="process">Process.</param>
        private static bool CheckTaskAndProcessMatch(JobTask task, Process process, out SwiftProcessCommandLine commandLine)
        {
            commandLine = null;

            if (task == null || process == null)
            {
                return false;
            }

            var job = task.Job;

            try
            {
                if (job.ExeType == "exe")
                {
                    LogWriter.Write("将按照DirectExe格式解析进程命令行", LogLevel.Debug);
                    commandLine = SwiftProcessCommandLine.Get(process, EnumExecutableFileType.DirectExe);
                }
                else if (job.ExeType == "dotnet")
                {
                    LogWriter.Write("将按照DotNet格式解析进程命令行", LogLevel.Debug);
                    commandLine = SwiftProcessCommandLine.Get(process, EnumExecutableFileType.DotNet);
                }
                else
                {
                    throw new NotSupportedException("not supported executable file type: " + job.ExeType);
                }

                if (commandLine == null)
                {
                    return false;
                }

                LogWriter.Write("已经成功解析进程命令行：" + JsonConvert.SerializeObject(commandLine), LogLevel.Trace);

                if (!commandLine.Paras.TryGetValue("-jn", out string jobName))
                {
                    jobName = string.Empty;
                }

                if (!commandLine.Paras.TryGetValue("-jr", out string jobRecordId))
                {
                    jobRecordId = string.Empty;
                }

                if (!commandLine.Paras.TryGetValue("-t", out string taskId))
                {
                    taskId = string.Empty;
                }

                if (jobName == job.Name && jobRecordId == job.Id && task.Id.ToString() == taskId)
                {
                    LogWriter.Write("进程命令和任务匹配", LogLevel.Debug);
                    return true;
                }

                LogWriter.Write("进程命令和任务不匹配", LogLevel.Debug);
            }
            catch (Exception ex)
            {
                LogWriter.Write("analysis process commndline go exception.", ex, LogLevel.Info);
            }

            return false;
        }
    }
}
