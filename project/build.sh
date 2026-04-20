#!/bin/sh
if [ "$1" = "build" ];then
    mkdir /home/changsheng/project/project10071/project
    cp -a /home/changsheng/project/project10071/server/. /home/changsheng/project/project10071/project/
    cd /home/changsheng/project/project10071/project
    rm -rf /home/changsheng/project/project10071/server
    echo "执行成功"
fi
