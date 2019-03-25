# 运行时基础镜像
FROM microsoft/dotnet:2.1-aspnetcore-runtime AS base
WORKDIR /app

# 复制sources.list
COPY sources.list .

# 安装Consul
ENV CONSUL_VERSION=1.4.2
ENV HASHICORP_RELEASES=https://releases.hashicorp.com
RUN set -eux && \
    rm -rf /etc/localtime && \
    ln -s /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    mv /etc/apt/sources.list /etc/apt/sources.list.bak && \
    mv /app/sources.list /etc/apt/ && \
    apt-get update && \
    apt-get install -y wget unzip iproute iproute-doc && \
    mkdir -p /tmp/build && \
    cd /tmp/build && \
    apkArch="$(lscpu | grep 'Architecture' | sed -e 's/^Architecture:[[:space:]]*//')" && \
    case "${apkArch}" in \
        aarch64) consulArch='arm64' ;; \
        armhf) consulArch='arm' ;; \
        x86) consulArch='386' ;; \
        x86_64) consulArch='amd64' ;; \
        *) echo >&2 "error: unsupported architecture: ${apkArch} (see ${HASHICORP_RELEASES}/consul/${CONSUL_VERSION}/)" && exit 1 ;; \
    esac && \
    wget ${HASHICORP_RELEASES}/consul/${CONSUL_VERSION}/consul_${CONSUL_VERSION}_linux_${consulArch}.zip && \
    mkdir /app/consul && \
    unzip -d /app/consul consul_${CONSUL_VERSION}_linux_${consulArch}.zip && \
    ln -s /app/consul/consul /usr/local/bin/consul && \
    cd /tmp && \
    rm -rf /tmp/build && \
    mkdir -p /app/consul/data && \
    mkdir -p /app/consul/config && \
    chown 777 /app/consul/data && \
    consul version

# 编译项目基础镜像
FROM microsoft/dotnet:2.1-sdk AS build
WORKDIR /src

# 复制项目主文件，还原项目依赖项
#COPY Swift/Swift.csproj Swift/
#COPY Swift.Core/Swift.Core.csproj Swift.Core/
#COPY Swift.Management/Swift.Management.csproj Swift.Management/

# 复制所有项目源文件，然后发布项目
COPY . .
RUN dotnet restore Swift/Swift.csproj
RUN dotnet restore Swift.Management/Swift.Management.csproj

FROM build AS publish

WORKDIR /src/Swift
#RUN dotnet build --configuration Release
RUN dotnet publish -c Release -o /app/swift

WORKDIR /src/Swift.Management
RUN dotnet publish -c Release -o /app/management

# 复制发布的项目文件到运行时目录，然后生成镜像
FROM base AS final
WORKDIR /app
COPY --from=publish /app/swift ./swift
COPY --from=publish /app/management ./management

# 开放端口
EXPOSE 8300 8301 8301/udp 8302 8302/udp 9631 9632

# 启动时执行的命令
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
ENTRYPOINT ["docker-entrypoint.sh"]