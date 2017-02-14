#!/bin/bash
Args=$@
ShellHome=/app/bi/medusa/bin/shell
$ShellHome/MoretvBIRun.sh "com.moretv.bi.overview.HelpManualStatics" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.overview.HomeAccessDistribution" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.overview.HomeAccessSubModule" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.overview.HomepageStatistics" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.overview.HotRecommendSanjiPagePVUVVV" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.overview.HotRecommendTagPagePVUVVV" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.overview.OtherWatchMap" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.overview.HomeAccessProgramPVUVVV" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.overview.TotalUVVVStatistics" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.overview.VideoTypeDurationStatistics" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.overview.VideoTypeUVVVStatistics" $Args
