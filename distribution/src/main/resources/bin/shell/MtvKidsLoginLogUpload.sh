#!/bin/bash

DAY=$1

HADOOP_HOME=/hadoopecosystem/hadoop

JAVA_HOME=/tools/jdk1.7.0_79

LogDir=/log/mtvkidsloginlog/rawlog

$HADOOP_HOME/bin/hadoop fs -put /var/ftp/mtvkidsloginlog/mtvkidsloginlog.access.log_"$DAY"* $LogDir
