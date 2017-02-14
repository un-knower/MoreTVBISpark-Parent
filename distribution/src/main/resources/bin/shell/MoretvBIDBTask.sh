#!/bin/bash
Day=`date -d 'today' +%Y%m%d`
Args="--startDate $Day --deleteOld true"
ShellHome=/app/bi/medusa/bin/shell
$ShellHome/MoretvBIRun.sh "com.moretv.bi.user.AppVersionUser" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.snapshot.DBSnapShot" $Args
