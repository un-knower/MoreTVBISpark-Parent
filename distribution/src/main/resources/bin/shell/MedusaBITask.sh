#!/bin/bash

ShellHome=/app/bi/medusa/bin/shell/
Day=`date -d 'today' +%Y%m%d`
Args="--startDate $Day --deleteOld true"

$ShellHome/MedusaBIRun.sh com.moretv.bi.medusa.log.Log2Parquet $Args --logTypes "play,detail,live,collect,deletecollect,enter,evaluate,exit,homeaccess,homeview,interview,livebutton,matchdetail,mtvaccount,pageview,playqos,playview,positionaccess,subject,subscribe,tabview,videobutton,appaccess,switchonoff,livecenterview,sportslivedetail,sportssubscribe,buttonclick,editentrance,entranceview,liveqos"
$ShellHome/MedusaBIRun.sh com.moretv.bi.medusa.log.Log2ParquetPost $Args
$ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.StartEndLogMerger $Args
$ShellHome/MedusaMoretvLogMergerRun.sh
$ShellHome/MedusaBIAllRun.sh


