#!/bin/bash

# https://blog.csdn.net/wdz306ling/article/details/79756968
# http://www.runoob.com/linux/linux-shell-variable.html

cluster=''
bindip=''
consulserver=false
role=''
n=0
until [ -z "$1" ]
do
    ((n += 1))
    #echo '$'$n="$1"
    IFS='=' read -r -a argarray <<< "$1"
    if [ "${argarray[0]}" == '-cluster' ]; then
        cluster="${argarray[1]}"
    fi
    if [ "${argarray[0]}" == '-consulserver' ]; then
        consulserver=true
    fi
    echo "${argarray[0]}"
    echo "${argarray[1]}"
    shift
done

echo "${cluster}"
echo "${consulserver}"