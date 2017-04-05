package com.moretv.bi.util

/**
 * Created by Will on 2015/8/5.
 */
@deprecated
object ProductModelUtil {

  def getProductBrand(productModel: String): String = {
    productModel match {
      case null => "NullNil"
      case _ =>{
        var idx = productModel.indexOf('_')
        if(idx < 0) idx = productModel.indexOf('-')
        if (idx > 0) {
          productModel.substring(0, idx)
        }else if (productModel.length > 20) {
          productModel.substring(0, 20)
        }else {
          productModel
        }
      }
    }
  }
}
