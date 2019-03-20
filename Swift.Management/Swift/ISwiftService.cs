using System.Collections.Generic;
using Microsoft.AspNetCore.Http.Internal;
using Swift.Core;

namespace Swift.Management.Swift
{
    public interface ISwiftService
    {
        List<Cluster> GetClusters();

        bool Run(string clusterName, string jobName);

        bool Cancel(string clusterName, string jobName, string jobId);

        Member GetManager(string clusterName);

        void RestartManagerElection(string clusterName);

        List<Member> GetMembers(string clusterName);

        List<JobBase> GetJobRecords(string clusterName, string jobName, System.DateTime? date);

        List<JobConfig> GetJobConfigs(string clusterName);

        bool PublishJobPackage(string clusterName, FormFile file);

        byte[] DownloadJobResult(string clusterName, string jobName, string jobId);
    }
}
