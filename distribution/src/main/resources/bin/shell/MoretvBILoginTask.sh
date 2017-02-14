#!/bin/bash

Day=`date -d 'today' +%Y%m%d`
Offset=1
Args="--startDate $Day --deleteOld true"
ShellHome=/app/bi/medusa/bin/shell
$ShellHome/MoreTVLoginLogUpload.sh $Day
$ShellHome/MtvKidsLoginLogUpload.sh $Day
$ShellHome/MoretvBIRun.sh "com.moretv.bi.logprocess.LoginLog2Parquet" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.login.GenerateMtvLoginLogData" $Args
$ShellHome/insert_login_detail_data_into_mysql.sh $Offset
$ShellHome/MoretvBIRun.sh "com.moretv.bi.kidslogin.LoginLog2Parquet" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.kidslogin.MtvKidsAccessUsers" $Args
$ShellHome/generate_mtv_kid_user_overview_everyday.sh $Offset
$ShellHome/generate_mtv_user_overview_everyday.sh $Offset
$ShellHome/MoretvBIRun.sh "com.moretv.bi.login.ApkSeriesVersionDist" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.login.PromotionChannelDetail" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.kidslogin.KidsPromotionChannelDetail" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.login.MtvPromotionChannelDetail" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.login.OpenApiUserTrend" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.login.AliDogTVDogActiveAndNewUserStatistics" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.login.YunOSLoginUser" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.login.AreaDist" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.login.CishuStatistics" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.login.LoginPeriodByApkSeriesVersion" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.login.ProductModelDist" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.login.ProductModelNewDist" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.login.YunOSAddUserBasedAlidogTvdog" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.login.YunOSActiveUserBasedAlidogTvdog" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.login.YunOSBasedPromotionChannel" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.user.ProductBrandDist" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.user.ProductBrandPromotionChannelNewDist" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.user.PromotionChannelNonOTTNewDist" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.user.PromotionChannelTotalDist" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.login.UserStatisticsBasedApkVersion" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.login.AdduserBasedApkVersionAndPromotionChannel" $Args
