# Swift

Swift是一个基于.NET Core的分布式批处理框架，支持将作业分割后分发到多台服务器并行处理，可成倍提升大量数据的处理速度。


## 原理

Swift作业处理分为3步：分割作业、执行任务、合并结果。

Swift由多个成员节点组成集群，成员分为Manager和Worker，Manager负责分割作业和合并结果，Worker负责执行具体的任务。Manager同时只有一个，自动选举产生，集群工作需要至少2个节点，节点数>=3才有意义。

Swift通过启动子进程来处理作业，原则上可以支持各类语言开发的程序，目前仅实现了.NET Core作业的支持。

Swift依赖[Consul](https://www.consul.io/)管理集群节点，每台部署了Swift节点的机器都需要部署Consul。

Swift作业的元数据都保存在集中的配置中心，各个节点从配置中心拉取最新的配置用于处理作业，以及将自身的工作状态实时更新到配置中心。


## Swfit使用

### 运行环境搭建

Swift基于.NET Core平台，可以运行在Windows、Linux、Mac等多种操作系统，动手能力强的同学可以在自己熟悉的环境手动部署。

不过使用docker可以快速创建一个Swift集群，省去下载、部署、配置等各种麻烦事，如果你的程序没有重度使用Consul，也可以考虑用于生产环境。

#### 1、docker

（1）使用解决方案中的Dockerfile生成Swift镜像：

```shell    
docker build -t fireflysoft/swift:latest .
```

（2）启动Swift容器：

没有Consul集群的情况下，为了方便测试，支持通过参数 -consulboot 启动Consul Agent，使之作为Consul集群的Server和Leader。
生产环境下为了数据安全，应该有专门的Consul Server节点。

```shell
docker run --name swift1 -d -p 9632:9632 fireflysoft/swift:latest -cluster=test -consulboot
```
参数 -cluster 指定了Swift集群的名称。这里同时将9632端口映射到了主机，这个是Swift管理界面程序的端口。

已经有Consul Server节点的情况下，这样启动容器：

```shell
docker run --name swift2 -d fireflysoft/swift:latest -cluster=test -consuljoinip=172.17.0.2
```
参数 -consuljoinip 指定了容器中的Consul节点要加入的Consul集群中的任意一个节点。

建议启动3个Swift容器，以方便进行测试。


#### 2、手动部署

（1）部署Consul

这里只是大概说下Consul的部署，具体的使用或问题请网络搜索，也欢迎加入Consul交流群讨论（234939415）。

在这里下载Consul程序包：https://www.consul.io/downloads.html

然后编写节点[配置](https://www.consul.io/docs/agent/options.html)，举个例子：集群dc1的192.168.0.2节点，它是一个server节点，通过节点192.168.0.3连接到集群，集群预料有3个Server节点。

```javascript
{
  "datacenter": "dc1",
  "data_dir": "/usr/local/consul/data",
  "node_name": "192.168.0.2",
  "advertise_addr": "192.168.0.2",
  "start_join": ["192.168.0.3"],
  "rejoin_after_leave": true,
  "server": true,
  "bootstrap-expect":3,
  "ui": true
}
```
然后使用命令启动这个节点：

```shell
consul agent -config-dir ./conf
```
测试环境1个Server节点就够了，生产环境建议启动3或5个Server节点。

（2）部署Swift

Swift当前有2个核心程序：节点程序（Swift）和管理界面程序（Swift.Management），前者是一个控制台程序，后者是一个Web程序。这两个程序部署起来很简单，需要注意当前机器上需要已经部署了Consul节点。

首先使用Visual Studio发布程序，然后部署到指定的目录，使用命令行启动。

启动节点程序：

```shell
dotnet /app/swift/Swift.dll -c swiftcluster
```

启动管理界面程序（只需要在其中1个机器部署即可）：

```shell
dotnet /app/management/Swift.Management.dll --urls "http://0.0.0.0:9632"
```
用户可以通过管理界面查看集群节点，上传作业包，监控作业运行，下载作业结果，也可以很方便的运行作业和取消作业。

Swift节点建议不要少于3个，最少2个。


### 创建作业

VS解决方案中提供了一个作业的例子：Swift.DemoJob，可以参考来创建自己的作业。

#### 1、编写作业

用户首先需要按照Swift的规范编写作业处理程序，作业处理程序包括三部分：分割任务、执行任务、合并任务。
这三部分对应到程序中是三个方法，Swift规定了方法的名称和输入输出类型，用户实现内部逻辑即可。

#### 2、打包作业

打包作业处理程序首先需要编译程序，建议使用命令：

```shell
dotnet publish -c Release
```
然后附加一个名为job.json的作业配置文件，其中指定作业的名称、可执行文件名、计划执行时间等信息：

```shell
{
  "Name": "DemoJob", // 作业名称
  "FileName": "Swift.JobEntryPoint.dll", // 作业可执行文件，目前固定为.NET Core对应入口文件，不要改动
  "ExeType": "dotnet", // 作业可执行文件类型，目前仅支持dotnet
  "JobClassName": "Swift.DemoJob.dll,Swift.DemoJob.DemoJob", // 作业所在文件和类的全名称
  "RunTimePlan": [ "10m" ], // 作业运行时间计划，可以指定多个
  "TaskExecuteTimeout": 1440, // 单个任务执行超时时间，默认1440分钟
  "MemberUnavailableThreshold": 10, // 节点不可用的认定阈值，默认10分钟
  "JobSplitTimeout":120, // 作业分割超时时间，默认120分钟
  "TaskResultCollectTimeout":120 // 任务结果合并超时时间，默认120分钟
}
```

运行时间计划格式说明：
- HH:mm 每天定时运行
- ddd HH:mm 每周定时运行
- MM-dd HH:mm 每月定时运行
- yyyy-MM-dd HH:mm 定时运行一次
- dH 每d小时执行一次
- dm 每m分钟执行一次

然后将这些全部打包到一个zip文件，文件名需要和job.json中的作业名称一致。

#### 3、运行作业

将打包的zip文件通过Swift管理界面上传，稍等几秒钟，Swift会自动发现作业包，并分发到集群中。
现在你可以直接点击运行作业，也可以等待作业按照计划执行。

## 后记

这个程序还很简单，代码写的也有些丑陋，还有很多设想没有实现，如有兴趣，欢迎Fork！