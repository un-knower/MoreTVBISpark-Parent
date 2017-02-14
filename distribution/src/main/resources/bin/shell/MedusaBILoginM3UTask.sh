#!/bin/bash

ShellHome=/app/bi/medusa/bin/shell/
Day=`date -d 'today' +%Y%m%d`
Args="--startDate $Day"

$ShellHome/MedusaBIRun.sh com.moretv.bi.medusa.login.DailyActiveUser $Args
$ShellHome/MedusaBIRun.sh com.moretv.bi.medusa.login.DailyActiveUserVersionDist $Args
$ShellHome/MedusaBIRun.sh com.moretv.bi.medusa.login.DayRetentionRate $Args
$ShellHome/MedusaBIRun.sh com.moretv.bi.medusa.login.DayRetentionRateEachProductModel $Args
