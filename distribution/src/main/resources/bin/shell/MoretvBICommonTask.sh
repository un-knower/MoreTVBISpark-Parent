#!/bin/bash
Args=$@
ShellHome=/app/bi/medusa/bin/shell
$ShellHome/MoretvBIRun.sh "com.moretv.bi.common.CommonSubTotalPVUV" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.common.CommonSubTotalTime" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.common.CommonTotalPVUV" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.common.CommonTotalTime" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.common.PeoplealsolikePVUV" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.common.AppRecommendInstall" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.common.AppRecommendPVUV" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.common.DoubanCommentPageView" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.common.LiveTimeShifting" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.common.MoretvCommentPageView" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.common.MutilSeasionOperation" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.common.PositionAccessPVUV" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.common.PrevueVVUV" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.common.Watchprevue" $Args
