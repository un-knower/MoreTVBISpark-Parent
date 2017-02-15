#!/bin/sh
day=`date -d 'today' +%Y%m%d`
ShellHome=/app/bi/medusa/bin/shell/
# ============Query subject info ===================
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.contentEvaluation.QuerySubjectPlayInfo --startDate $day --deleteOld true > /script/bi/medusa/bi/logs/querySubjectPlayInfo.log
# ============For newsroom kpi==========
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.newsRoomKPI.EachChannelPlayInfo --startDate $day --deleteOld true > //script/bi/medusa/bi/logs/eachChannelPlayInfo.log
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.contentEvaluation.QuerySubjectViewInfo --startDate $day --deleteOld true > /script/bi/medusa/bi/logs/querySubjectViewInfo.log
