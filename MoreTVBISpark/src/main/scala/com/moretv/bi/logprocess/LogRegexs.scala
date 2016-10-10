package com.moretv.bi.logprocess

/**
 * Created by Will on 2015/9/19.
 * This object contains a collect of regex, which is for one special situation.
 */
object LogRegexs {

  val regexPlayqos = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=play-001-(userexit|playend|playerror|sourceerror)-(MoreTV[\\w\\.]+)-" +
    "([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(.+)-(\\w+)-" +
    "(xiqu|mv|sports|movie|tv|zongyi|comic|kids|jilu|hot)-" +
    "([a-z0-9]{12})-([a-z0-9]{12})-(\\d+)-(\\d+)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

  val regexPlayview = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=play-001-(playview)-(MoreTV[\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(.+)-" +
    "(xiqu|mv|sports|movie|tv|zongyi|comic|kids|jilu|hot)-([a-z0-9]{12})-([a-z0-9]{12})-" +
    "(\\d+)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

  val regexPlayBaiduCloud = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=play-001-(playview|userexit|playend|playerror|sourceerror)-" +
    "(MoreTV[\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(home-baidu_cloud)-(.+)-" +
    "(\\d+)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

  val regexDetail = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=detail-001-(view)-(MoreTV[\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(.+)-" +
    "(movie|tv|zongyi|kids|comic|jilu)-([a-z0-9]{12})-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

  val regexDetailSubject = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=detail-001-(view)-(MoreTV[\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(.+)-" +
    "(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|hot\\d+|sports\\d+|xiqu\\d+|mv\\d+)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

  val regexCollect = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=collect-001-(ok|cancel)-(MoreTV[\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
    "-(\\w+)-(.+?)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r


  val regexOperationShowlivelist = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=operation-001-(showlivelist)-" +
    "(MoreTV[\\w\\.]+)-"
    + "([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(\\w+)-(\\d*)-(.+?)-(\\d{14})%26sid%3D([a-z0-9]{12})").r

  val regexOperationTimeshifting = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=operation-001-(timeshifting)-" +
    "(MoreTV[\\w\\.]+)-"
    + "([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(\\w+)-(\\d*)-(.+?)-(\\d{14})-sid\\*([a-z0-9]{12})").r

  val regexOperationEvaluate = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=operation-001-(evaluate)-" +
    "(MoreTV[\\w\\.]+)-"
    + "([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(\\w+)-(\\d*)-(.+?)-(\\d{14})-([a-z0-9]{12})-(\\w+)-(up|down)").r

  //This was for one which event was moretag or multiseason
  val regexOperationMM = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=operation-001-(moretag|multiseason)-" +
    "(MoreTV[\\w\\.]+)-"
    + "([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

  //This was for one which event was addtag, comment or watchprevue
  val regexOperationACW = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=operation-001-(addtag|comment|watchprevue|notrecommend)-" +
    "(MoreTV[\\w\\.]+)-"
    + "([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(\\w+)-(\\d*)-(.+?)-(\\d{14})-(\\w+)-([a-z0-9]{12})").r

  val regexLive = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=live-001-(MoreTV[\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-" +
    "([\\w\\-]+)-(\\d{1})-([a-z0-9]{12})-(\\d+)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

  val regexInterviewEnter = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=interview-001-enter-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
    "-(.+)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

  val regexInterviewExit = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=interview-001-exit-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
    "-(.+)-(\\d+)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

  val regexApprecommend = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=apprecommend-001-(\\w+)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
    "-([a-z0-9]{12}-app\\d+|[a-z0-9]{12}-|[a-z0-9]{12})-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

  val regexEnter = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=enter-001-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
    "(-thirdparty_\\d{1})?-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

  val regexExit = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=exit-001-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
    "(-thirdparty_\\d{1})?-(\\d+)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

  val regexHomeaccess = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=homeaccess-001-(\\w+)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
    "-(\\w+)-(\\d*)-(.+?)-(\\d{14})%26location%3D(\\d+)_(\\d+)").r

  val regexMtvaccount = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=mtvaccount-001-(\\w+)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
    "-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

  val regexMtvaccountLogin = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=mtvaccount-001-(login)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
    "-(\\w+)-(\\d*)-(.+?)-(\\d{14})-path\\*(\\w+)").r

  val regexPageview = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=pageview-001-(\\w+)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
    "-(\\w+)-(\\d*)-(.+?)-(\\d{14})(\\S*)").r

  val regexPositionaccess = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=positionaccess-001-(\\w+)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
    "-(\\w+)-(\\d*)-(.+?)-(\\d{14})-(\\d+)_(\\d+)").r

  val regexAppsubject = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=appsubject-001-(\\w+)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
    "-(\\w+)-(\\d*)-(.+?)-(\\d{14})-(\\w+)").r

  val regexStarpage = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=starpage-001-(\\w+)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
    "-(\\w+)-(\\d*)-(.+?)-(\\d{14})-star\\*([^\\-]+)-path\\*-([\\w\\-]+)").r

  val regexRetrieval = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=retrieval-001-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
    "-(.+)-(\\w+)-(\\w+)-(\\w+)-(\\w+)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

  val regexSetWallpaper = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=set-001-(wallpaper)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
    "-(\\w+)-(\\d*)-(.+?)-(\\d{14})%26wallpaper%3D(\\d+)").r

  val regexHomeRecommend = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=homerecommend-001-(access)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
    "-(\\w+)-(\\d*)-(.+?)-(\\d{14})-([^-]+)-([^-]+)-([^-]+)").r

  val regexHomeviewEnter = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=homeview-001-enter-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*)" +
    "-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

  val regexHomeviewExit = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=homeview-001-exit-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*)" +
    "-(\\w+)-(\\d*)-(.+?)-(\\d{14})-(\\d+)").r

  val regexDanmustatus = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=danmustatus-001-(on|off)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*)" +
    "-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

  val regexDanmuswitch = ("\\[([0-9]{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}).+?/uploadlog/\\?" +
    "log=danmuswitch-001-(on|off)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*)" +
    "-(\\w+)-(\\d*)-(.+?)-(\\d{14})-([a-z0-9]{12})-(\\w+)").r

}
