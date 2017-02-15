#!/bin/bash

SCALA_HOME=/tools/scala-2.10.4
Args=$@
ShellHome=/app/bi/medusa/bin/shell
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.BaiduYunUVVVPV" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.Channel_pv_uv" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.Channel_vv" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.KidsCatHousePVVVUV" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.KidsDurationAndUserNum" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.KidsSongUVVV" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.KidsSubchannelPVUVVUV" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.Live_period" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.Live_time" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.Live_user" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.LiveJingPinUVVV" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.ProgramType_uv_pv_vv" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.ProgramVV" $Args
$SCALA_HOME/bin/scala /script/bi/moretv/jar/ProgramVVTop500Day.jar $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.SubchannelPeriod" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.Third_path_pv_uv" $Args
$ShellHome/update_third_path_code_to_name.sh $Args third_path_pv_uv
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.Third_path_vv" $Args
$ShellHome/update_third_path_code_to_name.sh $Args third_path_vv
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.TotalAndAvgTime" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.TotalPVVV" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.Subject_pv_uv" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.ProgramViewAndPlayStats.Subject_vv" $Args

