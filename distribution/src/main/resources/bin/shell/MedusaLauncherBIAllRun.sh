#!/bin/sh
day=`date -d 'today' +%Y%m%d`
numOfDays=1
ShellHome=/app/bi/medusa/bin/shell
sh $ShellHome/MedusaBIRun.sh "com.moretv.bi.report.medusa.pageStatistics.LauncherViewStatistic" --startDate $day --numOfDays $numOfDays
sh $ShellHome/MedusaBIRun.sh "com.moretv.bi.report.medusa.pageStatistics.LauncherClickStatistic" --startDate $day --numOfDays $numOfDays
sh $ShellHome/MedusaBIRun.sh "com.moretv.bi.report.medusa.pageStatistics.LauncherPlayStatistic" --startDate $day --numOfDays $numOfDays
sh $ShellHome/MedusaBIRun.sh "com.moretv.bi.report.medusa.pageStatistics.LauncherSearchAndSetStatistic" --startDate $day --numOfDays $numOfDays
sh $ShellHome/MedusaBIRun.sh "com.moretv.bi.report.medusa.pageStatistics.LauncherRecommendStatistic" --startDate $day --numOfDays $numOfDays 
sh $ShellHome/MedusaBIRun.sh "com.moretv.bi.report.medusa.pageStatistics.LauncherMyTVStatistic" --startDate $day --numOfDays $numOfDays 
#sh $ShellHome/MedusaBIRun.sh "com.moretv.bi.report.medusa.pageStatistics.LauncherLiveStatistic" --startDate $day --numOfDays $numOfDays
sh $ShellHome/MedusaBIRun.sh "com.moretv.bi.report.medusa.pageStatistics.LauncherFoundationStatistic" --startDate $day --numOfDays $numOfDays
#sh $ShellHome/MedusaBIRun.sh "com.moretv.bi.report.medusa.pageStatistics.LauncherDifferentAreaClickStatistic" --startDate $day --numOfDays $numOfDays 
sh $ShellHome/MedusaBIRun.sh "com.moretv.bi.report.medusa.pageStatistics.LauncherClickBasedOnTimeStatistic" --startDate $day --numOfDays $numOfDays 
sh $ShellHome/MedusaBIRun.sh "com.moretv.bi.report.medusa.pageStatistics.LauncherClassificationStatistic" --startDate $day --numOfDays $numOfDays
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.channeAndPrograma.movie.SportPlayAndLiveTotalUserInfo --startDate $day --numOfDays $numOfDays 
