#!/bin/bash

ShellHome=/app/bi/medusa/bin/shell/
Day=`date -d 'today' +%Y%m%d`
Args="--startDate $Day"

$ShellHome/MedusaBIRun.sh com.moretv.bi.medusa.log.Log2Parquet $Args
$ShellHome/MedusaBIRun.sh com.moretv.bi.medusa.exit.UsageDurationM3U $Args
$ShellHome/MedusaBIRun.sh com.moretv.bi.medusa.play.VVDurationM3U $Args
$ShellHome/MedusaBIRun.sh com.moretv.bi.medusa.play.VVM3U $Args
