#!/bin/sh
day=`date -d 'today' +%Y%m%d`
ShellHome=/app/bi/medusa/bin/shell/
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.EvaluateLogMerger --startDate $day
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.InterviewLogMerger --startDate $day
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.CollectLogMerger --startDate $day
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.DetailLogMerger --startDate $day
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.HomeViewLogMerger --startDate $day
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.livebuttonLogMerger --startDate $day
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.videobuttonLogMerger --startDate $day
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.mtvaccountLogMerger --startDate $day
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.apprecommendLogMerger --startDate $day
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.ExitLogMerger --startDate $day
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.HomeAccessLogMerger --startDate $day
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.PlayViewLogMergerV2 --startDate $day
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.LiveLogMerger --startDate $day
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.PlayViewLogMerger --startDate $day
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.PlayViewLogMergerFilter --startDate $day
