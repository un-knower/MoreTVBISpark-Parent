#!/bin/bash

source ~/.bash_profile


Day=`date -d 'today' +%Y%m%d`

Args="--startDate $Day --numOfDays 1 --alarmFlag false --deleteOld true"

###### Search Info from searchEntrance , clickSearchResult, search-tabview

SearchEntranceStat=com.moretv.bi.report.medusa.functionalStatistic.searchInfo.SearchEntranceStat
SearchContentTypeStat=com.moretv.bi.report.medusa.functionalStatistic.searchInfo.SearchContentTypeStat
SearchEntranceContentTypeStat=com.moretv.bi.report.medusa.functionalStatistic.searchInfo.SearchEntranceContentTypeStat

SearchVideoContentTypeStat=com.moretv.bi.report.medusa.functionalStatistic.searchInfo.SearchVideoContentTypeStat

./submit.sh $SearchEntranceStat $Args
./submit.sh $SearchContentTypeStat $Args
./submit.sh $SearchEntranceContentTypeStat $Args
./submit.sh $SearchVideoContentTypeStat $Args



