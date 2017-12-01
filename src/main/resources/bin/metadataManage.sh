#!/bin/bash

cd `dirname $0`
pwd=`pwd`

while (( "$#" ));
do
    case $1 in
        --username)
            #azkaban用户名
            username=$2;
            ;;
        --password)
            #azkaban密码
            password=$2;
            ;;
        --path)
            #parquet路径
            path=$2;
            ;;
        --dbName)
            #hive数据库名称
            dbName=$2;
            ;;
        --tabPrefix)
            #hive表名前缀
            tabPrefix=$2;
            ;;
        --productCode)
            #所属产品线
            productCode=$2;
            ;;
        --appCode)
            #所属应用code
            appCode=$2;
            ;;
        --realLogType)
            #实际用于表名的logType或eventId或actionId
            realLogType=$2;
            ;;
        --startDate)
            #扫描开始日期
            startDate=$2;
            ;;
        --endDate)
            #扫描结束日期
            endDate=$2;
            ;;
        --startHour)
            #扫描开始时段
            startHour=$2;
            ;;
        --endHour)
            #扫描结束时段
            endHour=$2;
            ;;
        --numOfDays)
            #向前执行的天数（兼容老程序）
            numOfDays=$2;
            ;;
        --offset)
            #字段偏移量
            offset=$2;
            ;;
        --step)
            #扫描递增步长
            step=$2;
            ;;
    esac
shift
done

username=${username:-dw}
password=${password:-dw@whaley}

if [ $step = "hour" ];
then
    numOfDays=""
fi

if [ -z $numOfDays ];
then
    ((startOffset=1))
else
    ((startOffset=$numOfDays+1))
fi

startTime=`date -d "$startDate $startHour -$startOffset $step" +"%Y%m%d%H"`
endTime=`date -d "$endDate $endHour -1 $step" +"%Y%m%d%H"`



while [[ ${startTime}  -le  ${endTime} ]]
   do
    echo "execute time ... is ${startTime}"
    startDate=${startTime:0:8}
    startHour=${startTime:8:2}
    sh ../bin/curl.sh metadataManage metadataManage username ${username} password ${password} path ${path} dbName ${dbName} tabPrefix ${tabPrefix} productCode ${productCode} appCode ${appCode} realLogType ${realLogType} keyDay ${startDate} keyHour ${startHour} offset ${offset} deleteOld false
    if [ $? -ne 0 ];then
            echo "batch forest ${startTime} is fail ..."
            exit 1
    fi
    startTime=`date -d "${startDate} ${startHour} 1 $step" +"%Y%m%d%H"`
done
