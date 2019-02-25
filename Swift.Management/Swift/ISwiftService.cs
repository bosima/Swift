using System.Collections.Generic;
using Microsoft.AspNetCore.Http.Internal;
using Swift.Core;

namespace Swift.Management.Swift
{
    public interface ISwiftService
    {
        List<Cluster> GetClusters();

        List<Member> GetMembers(string clusterName);

        List<JobBase> GetJobRecords(string clusteName, string job);

        List<JobConfig> GetJobs(string clusterName);

        bool PublishJob(string clusterName,FormFile file);
    }
}
