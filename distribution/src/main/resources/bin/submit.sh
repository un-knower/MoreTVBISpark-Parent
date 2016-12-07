#!/bin/bash

source ~/.bash_profile

#set -x

Params=($@)
MainClass=${Params[0]}
Length=${#Params[@]}
Args=${Params[@]:1:Length-1}

cd `dirname $0`
pwd=`pwd`


for file in ../conf/*
do
	if [ -n "$resFiles" ]; then
		resFiles="$resFiles,$file"
	else
		resFiles="$file"
    fi
done

for file in ../lib/*.jar
do
	if [ -n "$jarFiles" ]; then
		jarFiles="$jarFiles,$file"
	else
		jarFiles="$file"
	fi
done

SPARK_HOME=/hadoopecosystem/spark
SparkMaster="spark://10.10.2.14:7077"
JarHome="../lib/MoreTVBISpark-1.0.0-SNAPSHOT.jar"
set -x
$SPARK_HOME/bin/spark-submit \
--name ${app_name:-$MainClass} \
--master $SparkMaster \
--executor-memory 5g --driver-memory 5g --conf spark.cores.max=75  \
--jars $jarFiles \
--files $resFiles \
--class "$" $JarHome $ArgsMainClass