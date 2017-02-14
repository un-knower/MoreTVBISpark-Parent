#!/bin/bash

DAY=$1

HADOOP_HOME=/hadoopecosystem/hadoop

JAVA_HOME=/tools/jdk1.7.0_79

LogDir=/log/moretvloginlog/rawlog/$DAY
#rename medusa login logs
#mv /var/ftp/loginlog/u.access.log_$DAY-login1.bz2 /var/ftp/loginlog/loginlog.access.log_$DAY-u1.bz2
#mv /var/ftp/loginlog/u.access.log_$DAY-login2.bz2 /var/ftp/loginlog/loginlog.access.log_$DAY-u2.bz2
$HADOOP_HOME/bin/hadoop fs -mkdir $LogDir
$HADOOP_HOME/bin/hadoop fs -put /var/ftp/loginlog/loginlog.access.log_"$DAY"* $LogDir
$HADOOP_HOME/bin/hadoop fs -put /var/ftp/loginlog/loginlog.access.log_"$DAY"* /log/loginlog/
