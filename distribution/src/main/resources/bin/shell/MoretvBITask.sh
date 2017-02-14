#!/bin/bash
Day=`date -d 'today' +%Y%m%d`
Args="--startDate $Day --deleteOld true"
Month=`date -d last-month +%Y%m`
num=1

#put file to hdfs,and convert file to parquet
sh /app/bi/medusa/bin/Log2Parquet.sh $Args
sh /app/bi/medusa/bin/Log2Parquet4Metis.sh $Args
sh /app/bi/medusa/bin/Log2ParquetForDanmu.sh $Args

#tag
sh /app/bi/medusa/bin/MoretvBITagTask1.sh $Args

#ProgramViewAndPlayStats
sh /app/bi/medusa/bin/MoretvBIProgramViewAndPlayStatsTask.sh $Args

#common
sh /app/bi/medusa/bin/MoretvBICommonTask.sh $Args

#content
sh /app/bi/medusa/bin/MoretvBIContentTask.sh $Args

#overview
sh /app/bi/medusa/bin/MoretvBIOverviewTask.sh $Args

#sports
sh /app/bi/medusa/bin/MoretvBISportsTask.sh $Args

#history
sh /app/bi/medusa/bin/MoretvBIHistoryTask.sh $Args

#operation
sh /app/bi/medusa/bin/MoretvBIOperationTask.sh $Args


#user
sh /app/bi/medusa/bin/MoretvBIUserTask.sh $Args
sh /app/bi/medusa/bin/MoretvBIUserTrendTask.sh $Args

#operation
sh /app/bi/medusa/bin/MoretvBIOperationTask.sh $Args

#VideoPlayErrorAnalyze
sh /app/bi/medusa/bin/MoretvBIVideoPlayErrorAnalyzeTask.sh $Args

#set
sh /app/bi/medusa/bin/MoretvBISetTask.sh $Args

#apprecomment
sh /app/bi/medusa/bin/MoretvBIAppRecommendTask.sh $Args

#account
sh /app/bi/medusa/bin/MoretvBIAccountTask.sh $Args












