#!/bin/sh
day=`date -d 'today' +%Y%m%d`
ShellHome=/app/bi/medusa/bin/shell/
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.userDevelop.totalDailyActiveUserInfo --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.channeAndPrograma.movie.EachVideoPlayInfo2ES --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.channeAndPrograma.movie.EachVideoOfChannelPlayInfo --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.channeAndPrograma.movie.EachChannelSubjectPlayInfo --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.contentEvaluation.todayRecommendationProgramPlayInfoQuery --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.channeAndPrograma.movie.KidsEachTabPlayInfo --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.channeAndPrograma.movie.KidsEachTabViewInfo --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.channeAndPrograma.movie.EachSubjectPlayInfo --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.channeAndPrograma.movie.EachDonghuaAndSongOfKidsPlayInfo --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.functionalStatistic.SearchProgramInfo --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.research.EachUserVideoPlayInfo --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.channeAndPrograma.movie.SportPlayAndLiveTotalUserInfo --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands.QueryYunOSPromotionNewAddUserInfo --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.channeAndPrograma.mv.EachVideoOfMVPlayInfo --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.functionalStatistic.appRecommendInstallInfo --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.channeAndPrograma.mv.EachMVPlayTopBoard --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.channeAndPrograma.mv.MVEachAreaPlayInfo --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.channeAndPrograma.mv.MVRecommendPlay --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.functionalStatistic.appRecommendInstallInfo --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.functionalStatistic.appRecommendOpenInfo --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.functionalStatistic.appRecommendViewInfo --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.functionalStatistic.appRecommendUpgradeInfo --startDate $day --deleteOld true
sh $ShellHome/MedusaBIRun.sh com.moretv.bi.report.medusa.userDevelop.accumulateUser --startDate $day --deleteOld true
