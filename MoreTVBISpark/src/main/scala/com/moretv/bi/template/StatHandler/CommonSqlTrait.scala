package com.moretv.bi.template.StatHandler

/**
  * Created by witnes on 11/3/16.
  */
trait CommonSqlTrait extends FieldTrait with EventTrait {


  /// select fields

  var selectFields = ""

  /// group fields

  var groupFields = ""

  /// having fields

  var havingFields = ""

  /// join fields

  var joinField = ""

  var questionField = ""

  var events = ""

  var timeType = ""


}
