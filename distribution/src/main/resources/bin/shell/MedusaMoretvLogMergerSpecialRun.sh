#!/bin/sh
day=`date -d 'today' +%Y%m%d`
ShellHome=/app/bi/medusa/bin/shell/
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.EntertogMerger --startDate $day
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.SubjectInterviewLogMerger --startDate $day
