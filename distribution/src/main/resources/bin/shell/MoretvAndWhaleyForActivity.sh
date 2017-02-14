#!/bin/bash
Day=`date -d 'today' +%Y%m%d`
Args="--startDate $Day --deleteOld true"
#Month=`date -d last-month +%Y%m`
#num=1

#activity class
#Log2Parquet
/app/bi/medusa/bin/shell/MoretvBIRun.sh "com.moretv.bi.logprocess.Log2ParquetForActivityNew" $Args
#/script/bi/moretv/shell/MoretvBIRun.sh "com.moretv.bi.activity.MidAutumnActivityTest" $Args


#MidAutumn Activity 










