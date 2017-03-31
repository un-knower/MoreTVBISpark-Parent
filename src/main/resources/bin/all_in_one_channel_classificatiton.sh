#!/usr/bin/env bash

alias fget='python /data/tscripts/scripts/ftp.py -s get -f '
fget MoreTVBISpark-1.0.0-michael.jar
mv MoreTVBISpark-1.0.0-michael.jar ./../lib/MoreTVBISpark-1.0.0.jar
md5sum ./../lib/MoreTVBISpark-1.0.0.jar

one_day=$1
echo "all in one"
sh submit.sh com.moretv.bi.report.medusa.channelClassification.ChannelClassificationStatETL --startDate ${one_day} --deleteOld true
