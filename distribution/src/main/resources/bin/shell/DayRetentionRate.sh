#!/bin/bash

source ~/.bash_profile
DAY=`date -d 'today' +%Y%m%d`
Args="--startDate $DAY"
/app/bi/medusa/bin/shell/MoretvBIRun.sh "com.moretv.bi.retention.DayRetentionRate"  $Args
