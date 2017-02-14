#!/bin/bash
Args=$@
/app/bi/medusa/bin/shell/MoretvBIRun.sh "com.moretv.bi.set.OptimizationToolsUsage" $Args
/app/bi/medusa/bin/shell/MoretvBIRun.sh "com.moretv.bi.set.WallpaperSetUsage" $Args


