#!/bin/sh
day=`date -d 'today' +%Y%m%d`
lastDay=`date -d 'yesterday' +%Y%m%d`
version='3.1.2'
ShellHome=/app/bi/medusa/bin/shell/
#sh /script/bi/medusa/bi/shell/MedusaBIRun.sh com.moretv.bi.report.medusa.productUpdateEvaluate.crash.CrashTrendsByVersionProductCrashKey --startDate $lastDay --apkVersion $version
#/script/bi/medusa/MoreTVBISpark-1.0.0-SNAPSHOT-bin/bin/submit.sh com.moretv.bi.report.medusa.productUpdateEvaluate.crash.CrashTrendsByVersionProductCrashKey --startDate $lastDay --apkVersion $version
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.productUpdateEvaluate.crash.CrashTrendsByVersionProductCrashKeyV2 --startDate $lastDay --apkVersion $version
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.productUpdateEvaluate.crash.MedusaDailyActivityInfo --startDate $day --apkVersion $version

# 统计所有videoSid的播放质量
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.medusa.playqos.PlayCodeVideoSourceStatics --startDate $day
# 统计不同videoSid的播放质量
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.dataAnalytics.PlayCodeVideoSourceStatics --startDate $day
#sh /script/bi/medusa/MoreTVBISpark-1.0.0-SNAPSHOT-bin/bin/submit.sh com.moretv.bi.report.medusa.CrashLog.CrashOriginalInfoNew --startDate $lastDay
#/script/bi/medusa/MoreTVBISpark-1.0.0-SNAPSHOT-bin/bin/submit.sh com.moretv.bi.report.medusa.CrashLog.CrashOriginalInfoNew --startDate $lastDay
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.CrashLog.EachUserEachCrashAppearInfo --startDate $lastDay
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.CrashLog.CrashToParquet --startDate $lastDay --deleteOld true
