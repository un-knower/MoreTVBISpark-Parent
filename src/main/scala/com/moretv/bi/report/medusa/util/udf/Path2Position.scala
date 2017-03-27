package com.moretv.bi.report.medusa.util.udf

/**
  * Created by witnes on 3/21/17.
  */

/**
  *  从path中 提取 position
  */
object Path2Position {


//  def getContentType()

  def isRecommended(recommendType: String): Boolean = {

    if (recommendType != null && recommendType != "") {
      true
    }
    else {
      false
    }
  }


  /**
    * In general, page contains many parts ,which means it holds different classifications.
    *
    * @param pagePath
    * @param isRecommended
    */
  def entranceFrom(contentType: String, pagePath: String, isRecommended: Boolean): Seq[String] = {

    /**
      * if the contentType is in the last location of pageLocation :
      * then this page level is the upstream of this contentType homepage.
      *
      * else if the contentType is in the first location of pageLocation :
      * then this page level is exactly this contentType homepage.
      *
      * and if isRecommended is True:
      * then this entrance is in the recommendation area parts.
      * else
      * this entrance is in the contentType site tree.
      * and this entrance may have its own downstram pages.
      *
      */


    // home page , exp: home*classification*mv

    if (pagePath.contains(contentType)) {

      val pathLocations = pagePath.split("\\*")


      if (pagePath.endsWith(contentType) && pathLocations.length >= 2) {

        // get the location before this contentType flag

        pathLocations(pathLocations.length - 2) :: Nil

      }
      // contentType homepage

      else if (pagePath.startsWith(contentType) && isRecommended && pathLocations.length >= 2) {

        // * *followed by sid , exp: mv*mvRecommendHomePage*34bdwxl7a19v
        pathLocations(1) :: Nil
      }

      else if (pagePath.startsWith(contentType) && !isRecommended && pathLocations.length >= 2) {

        // * * not followed by sid , exp: mv*function*site_hotsinger

        pathLocations.slice(1, pathLocations.length)

      }

      contentType :: Nil

    }
    else {
      // first level
      pagePath.split("\\*")(0) :: Nil
    }

  }


}
