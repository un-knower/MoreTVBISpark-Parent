#!/bin/bash

source ~/.bash_profile


#set -x

Params=($@)
MainClass=${Params[0]}
Length=${#Params[@]}
Args=${Params[@]:1:Length-1}

cd `dirname $0`
pwd=`pwd`

source ./envFn.sh

load_properties ../conf/spark.properties

#params: $1 className, $2 propName
getSparkProp(){
    className=$1
    propName=$2

    defaultPropKey=${propName}
    defaultPropKey=${defaultPropKey//./_}
    defaultPropKey=${defaultPropKey//-/_}
    #echo "defaultPropValue=\$${defaultPropKey}"
    eval "defaultPropValue=\$${defaultPropKey}"

    propKey="${className}_${propName}"
    propKey=${propKey//./_}
    propKey=${propKey//-/_}
    eval "propValue=\$${propKey}"

    if [ -z "$propValue" ]; then
        echo "$defaultPropValue"
    else
        echo "$propValue"
    fi
}

spark_driver_memory=$(getSparkProp $MainClass "spark.driver-memory")
spark_executor_memory=$(getSparkProp $MainClass "spark.executor-memory")
spark_cores_max=$(getSparkProp $MainClass "spark.cores.max")
spark_home=${spark_home:-$SPARK_HOME}
spark_master=${spark_master}
spark_mainJar=${spark_mainJar}


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

set -x
$spark_home/bin/spark-submit \
--name ${app_name:-$MainClass} \
--master $spark_master \
--executor-memory $spark_executor_memory \
--driver-memory $spark_driver_memory \
--conf spark.cores.max=$spark_cores_max  \
--jars $jarFiles \
--files $resFiles \
--class "$MainClass" $spark_mainJar $Args