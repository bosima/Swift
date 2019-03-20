using System;
using System.Diagnostics;

namespace Swift.Core
{
    /// <summary>
    /// Swift进程事件
    /// </summary>
    public class SwiftProcessEventActions
    {
        public Action<SwiftProcess, DataReceivedEventArgs> OutputAction { get; set; }
        public Action<SwiftProcess, DataReceivedEventArgs> ErrorAction { get; set; }
        public Action<SwiftProcess, EventArgs> StartedAction { get; set; }
        public Action<SwiftProcess, EventArgs> ExitAction { get; set; }
        public Action<SwiftProcess, EventArgs> TimeoutAction { get; set; }
    }
}
