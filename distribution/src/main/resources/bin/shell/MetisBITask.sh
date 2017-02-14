#!/bin/bash
Day=`date -d 'today' +%Y%m%d`
Args="--startDate $Day --deleteOld true"
num=1

/app/bi/medusa/bin/shell/MoretvBIRun.sh "com.moretv.bi.metis.ArticleDetailClickBasedEntrance" $Args
/app/bi/medusa/bin/shell/MoretvBIRun.sh "com.moretv.bi.metis.FunctionUseStatistics" $Args











