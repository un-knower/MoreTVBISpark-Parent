#!/usr/bin/env bash

if [ $# -lt 1 ]; then
  echo "需要输入日期 like 20161201"
  exit 1
fi

one_day=$1
echo "开始生成大宽表"
cd /script/bi/medusa/michael/MoreTVBISpark-1.0.0-SNAPSHOT-bin/bin
sh /script/bi/medusa/michael/MoreTVBISpark-1.0.0-SNAPSHOT-bin/bin/submit.sh  com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.PlayViewLogDimension --startDate ${one_day} --deleteOld true

echo "过滤播放次数大于指定数值的记录"
cd /script/bi/medusa/michael/MoreTVBISpark-1.0.0-SNAPSHOT-bin/bin
sh /script/bi/medusa/michael/MoreTVBISpark-1.0.0-SNAPSHOT-bin/bin/submit.sh  com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.PlayViewLogDimensionFilter --startDate ${one_day} --deleteOld true

echo "生成事实表和维度的每日信息"
cd /script/bi/medusa/michael/MoreTVBISpark-1.0.0-SNAPSHOT-bin/bin
sh /script/bi/medusa/michael/MoreTVBISpark-1.0.0-SNAPSHOT-bin/bin/submit.sh  com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.PlayViewLogDimensionExchange --startDate ${one_day} --deleteOld true

echo  "维度的每日信息和线上维度merge"
cd /script/bi/medusa/michael/MoreTVBISpark-1.0.0-SNAPSHOT-bin/bin
sh /script/bi/medusa/michael/MoreTVBISpark-1.0.0-SNAPSHOT-bin/bin/submit.sh com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.PlayViewLogDimensionMerge --startDate ${one_day} --deleteOld true --numOfDays=1


