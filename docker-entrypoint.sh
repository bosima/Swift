#!/bin/bash
# Swift cluster name\Swift member role、is consul bootstrap mode、consul join ip
cluster=''
role=''
consulboot=false
consuljoinip=''

# parameters
until [ -z "$1" ]
do
    IFS='=' read -r -a argarray <<< "$1"
    if [ "${argarray[0]}" == '-cluster' ]; then
        cluster="${argarray[1]}"
    fi
    if [ "${argarray[0]}" == '-role' ]; then
        role="${argarray[1]}"
    fi
    if [ "${argarray[0]}" == '-consulboot' ]; then
        consulboot=true
    fi
    if [ "${argarray[0]}" == '-consuljoinip' ]; then
        consuljoinip="${argarray[1]}"
    fi
   
    shift
done

[[ "${cluster}"=='' ]] && echo "Need Swift Cluster Name" && exit 1
[[ "${role}"=='' ]] && echo "Need Swift Member Role" && exit 1

#consul
CONSUL_DATA_DIR="/app/consul/data"
CONSUL_BIND_INTERFACE="eth0"
CONSUL_BIND=
if [ -n "$CONSUL_BIND_INTERFACE" ]; then
  CONSUL_BIND_ADDRESS=$(ip -o -4 addr list $CONSUL_BIND_INTERFACE | head -n1 | awk '{print $4}' | cut -d/ -f1)
  if [ -z "$CONSUL_BIND_ADDRESS" ]; then
    echo "Could not find IP for interface '$CONSUL_BIND_INTERFACE', exiting"
    exit 1
  fi

  CONSUL_BIND="-bind=$CONSUL_BIND_ADDRESS"
  echo "==> Found address '$CONSUL_BIND_ADDRESS' for interface '$CONSUL_BIND_INTERFACE', setting consul bind option..."
fi

CONSUL_CLIENT=
if [ -n "$CONSUL_CLIENT_INTERFACE" ]; then
  CONSUL_CLIENT_ADDRESS=$(ip -o -4 addr list $CONSUL_CLIENT_INTERFACE | head -n1 | awk '{print $4}' | cut -d/ -f1)
  if [ -z "$CONSUL_CLIENT_ADDRESS" ]; then
    echo "Could not find IP for interface '$CONSUL_CLIENT_INTERFACE', exiting"
    exit 1
  fi

  CONSUL_CLIENT="-client=$CONSUL_CLIENT_ADDRESS"
  echo "==> Found address '$CONSUL_CLIENT_ADDRESS' for interface '$CONSUL_CLIENT_INTERFACE', settin consul client option..."
fi

CONSUL_BOOT=
if  [ "${consulboot}"=='true' ]; then
    CONSUL_BOOT="-bootstrap -server"
fi

CONSUL_JOIN=
if  [ -n "${consuljoinip}" ]; then
    CONSUL_JOIN="-retry-join "${consuljoinip}""
fi

#consulcmd="consul agent ${CONSUL_BIND} ${CONSUL_CLIENT} -data-dir=${CONSUL_DATA_DIR} ${CONSUL_JOIN} ${CONSUL_BOOT}"
#exec "${consulcmd}"
consul agent ${CONSUL_BIND} ${CONSUL_CLIENT} -data-dir=${CONSUL_DATA_DIR} ${CONSUL_JOIN} ${CONSUL_BOOT}

#swift
#swiftcmd="dotnet /app/swift/Swift.dll -c ${cluster} -r ${role}"
#exec "${swiftcmd}"
dotnet /app/swift/Swift.dll -c ${cluster} -r ${role}

#swiftmanagementcmd="dotnet /app/management/Swift.Management.dll --server.urls \"http://localhost:9632\""
#exec "${swiftmanagementcmd}"
dotnet /app/management/Swift.Management.dll --server.urls "http://localhost:9632"