#!/bin/sh
if [ $# -ne 4 ]; then
    echo " 请输出 4 个参数 date  pathtype  logtype offset"
	echo " usage : `basename  $0`  date  pathtype  logtype offset"
    exit 1
fi

date=$1
pathtype=$2
logtype=$3
offset=$4
date=`date -d "-$offset days "$date +%Y%m%d`
case $pathtype in
 "whaley")
  path=/log/whaley/parquet/$date/$logtype/_SUCCESS
  ;;
 "dbsnapshot")
  path=/log/dbsnapshot/parquet/$date/$logtype/_SUCCESS 
 ;;
 "moretv")
  path=/mbi/parquet/$logtype/$date/_SUCCESS 
  ;;
  "medusa")
  path=/log/medusa/parquet/$date/$logtype/_SUCCESS
  ;;
  "merger")
  path=/log/medusaAndMoretvMerger/$date/$logtype/_SUCCESS
  ;;
  "loginlog")
  path=/log/moretvloginlog/parquet/$date/$logtype/_SUCCESS
  ;;
  "mtvkidslogin")
  path=/log/mtvkidsloginlog/parquet/$date/$logtype/_SUCCESS
  ;;
esac

hadoop fs -test -e $path
if [ $? -eq 0 ] ; then
     echo "$path is exist" 
else 
     echo "Error ! $path is not exist "
	 exit 2
fi