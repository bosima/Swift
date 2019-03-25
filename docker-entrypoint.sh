#!/bin/bash

cluster=''
consulboot=0
consuljoinip=''

until [ -z "$1" ]
do
    IFS='=' read -ra argarray <<< "$1"
    if [ "${argarray[0]}" == '-cluster' ]; then
        cluster="${argarray[1]}"
    fi
    if [ "${argarray[0]}" == '-consulboot' ]; then
        consulboot=1
    fi
    if [ "${argarray[0]}" == '-consuljoinip' ]; then
        consuljoinip="${argarray[1]}"
    fi
    shift
done

if [ ! -n "$cluster" ]; then
    echo "Need Swift Cluster Name" && exit 1
fi

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

CONSUL_BOOT=''
if  [ "${consulboot}" = '1' ]; then
    CONSUL_BOOT="-bootstrap -server"
fi

CONSUL_JOIN=
if  [ -n "${consuljoinip}" ]; then
    CONSUL_JOIN="-retry-join "${consuljoinip}""
fi

echo "${CONSUL_BOOT}"
echo "consul is starting"
nohup consul agent -datacenter=swiftdc ${CONSUL_BIND} ${CONSUL_CLIENT} -data-dir=${CONSUL_DATA_DIR} ${CONSUL_JOIN} ${CONSUL_BOOT} > /app/consul/log.txt 2>&1 &
sleep 15s
echo "consul has started"

cd /app/management/
nohup dotnet /app/management/Swift.Management.dll --urls "http://0.0.0.0:9632" 2> /app/swift/error.txt > /dev/null &
echo "management has started"

nohup dotnet /app/swift/Swift.dll -c ${cluster} 2> /app/swift/error.txt > /dev/null &
echo "swift has started"

# https://github.com/vearne/graceful_docker/blob/master/entrypoint.sh

while true; do echo hello world; sleep 3; done