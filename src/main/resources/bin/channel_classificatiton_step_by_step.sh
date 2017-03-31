#!/usr/bin/env bash

alias fget='python /data/tscripts/scripts/ftp.py -s get -f '
fget MoreTVBISpark-1.0.0-michael.jar
mv MoreTVBISpark-1.0.0-michael.jar ./../lib/MoreTVBISpark-1.0.0.jar
md5sum ./../lib/MoreTVBISpark-1.0.0.jar

one_day=$1
echo "戏曲"
sh submit.sh com.moretv.bi.report.medusa.channelClassification.OperaChannelClassificationStat --startDate ${one_day} --deleteOld true

echo "记录"
sh submit.sh com.moretv.bi.report.medusa.channelClassification.RecordChannelClassificationStat --startDate ${one_day} --deleteOld true

echo "综艺"
sh submit.sh com.moretv.bi.report.medusa.channelClassification.VarietyProgramChannelClassificationStat --startDate ${one_day} --deleteOld true

echo "动漫"
sh submit.sh com.moretv.bi.report.medusa.channelClassification.ComicChannelClassificationStat --startDate ${one_day} --deleteOld true

echo "资讯"
sh submit.sh com.moretv.bi.report.medusa.channelClassification.HotChannelClassificationStat --startDate ${one_day} --deleteOld true

echo "电视"
sh submit.sh com.moretv.bi.report.medusa.channelClassification.TvChannelClassificationStat --startDate ${one_day} --deleteOld true

echo "电影"
sh submit.sh com.moretv.bi.report.medusa.channelClassification.TvChannelClassificationStat --startDate ${one_day} --deleteOld true