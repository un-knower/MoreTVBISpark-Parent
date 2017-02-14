#!/bin/bash
Args=$@
ShellHome=/app/bi/medusa/bin/shell
$ShellHome/MoretvBIRun.sh "com.moretv.bi.account.TotalUsersByAccount" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.account.UseAccountAboutUserDistribute" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.account.AccountAccess" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.account.AccountLogin" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.account.AccountLoginTotalUser" $Args

