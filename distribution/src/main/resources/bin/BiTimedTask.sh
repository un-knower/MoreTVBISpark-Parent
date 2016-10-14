#!/bin/bash

source ~/.bash_profile


Day=`date -d 'today' +%Y%m%d`

Args="--startDate $Day --numOfDays 1 --alarmFlag false --deleteOld true"


###### Search Info from searchEntrance , clickSearchResult, search-tabview

SearchEntranceStat=com.moretv.bi.report.medusa.functionalStatistic.searchInfo.SearchEntranceStat
SearchContentTypeStat=com.moretv.bi.report.medusa.functionalStatistic.searchInfo.SearchContentTypeStat
SearchEntranceContentTypeStat=com.moretv.bi.report.medusa.functionalStatistic.searchInfo.SearchEntranceContentTypeStat

SearchVideoContentTypeStat=com.moretv.bi.report.medusa.functionalStatistic.searchInfo.SearchVideoContentTypeStat

#Medusa BI MV Stat

MVTabViewStat=com.moretv.bi.report.medusa.channeAndPrograma.mv.af310.MVTabViewStat
MVTabPlayStat=com.moretv.bi.report.medusa.channeAndPrograma.mv.af310.MVTabPlayStat

MVOminibusStat=com.moretv.bi.report.medusa.channeAndPrograma.mv.af310.MVOminibusStat
MVOminibusSrcStat=com.moretv.bi.report.medusa.channeAndPrograma.mv.af310.MVOminibusSrcStat

MVVideoStat=com.moretv.bi.report.medusa.channeAndPrograma.mv.af310.MVVideoStat
MVVideoSrcStat=com.moretv.bi.report.medusa.channeAndPrograma.mv.af310.MVVideoSrcStat

MVPositionClickStat=com.moretv.bi.report.medusa.channeAndPrograma.mv.af310.MVPositionClickStat

#Medusa BI Kid Stat

KidViewClass=com.moretv.bi.medusa.temp.KidsEachTabViewInfo
KidPlayClass=com.moretv.bi.medusa.temp.KidsEachTabPlayInfo


#Medusa dianbo

DetailContentTypeStat=com.moretv.bi.report.medusa.contentEvaluation.DetailContentTypeStat
PlayContentTypeStat=com.moretv.bi.report.medusa.contentEvaluation.PlayContentTypeStat

####

#Medusa BI StartPage Stat

StartPageStatistics=com.moretv.bi.report.medusa.pageStatistics.StartPageStatistics



./submit.sh $DetailContentTypeStat $Args
./submit.sh $PlayContentTypeStat $Args

./submit.sh $MVPositionClickStat $Args

./submit.sh $MVTabViewStat $Args
./submit.sh $MVTabPlayStat $Args

./submit.sh $MVOminibusStat $Args
./submit.sh $MVOminibusSrcStat $Args

./submit.sh $MVVideoStat $Args
./submit.sh $MVVideoSrcStat $Args

./submit.sh $KidViewClass $Args
./submit.sh $KidPlayClass $Args

#./submit.sh $SearchEntranceStat $Args
#./submit.sh $SearchContentTypeStat $Args
#./submit.sh $SearchEntranceContentTypeStat $Args
#./submit.sh $SearchVideoContentTypeStat $Args
#./submit.sh $$StartPageStatistics $Argsz