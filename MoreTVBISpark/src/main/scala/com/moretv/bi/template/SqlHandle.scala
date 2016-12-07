package com.moretv.bi.template

/**
  * Created by witnes on 11/15/16.
  */
trait SqlHandle {

  private val tableName = ""

  private val fields  = ""

  private val insertSql = s"insert into $tableName($fields)values()"

  private val deleteSql = ""

  def execute = {


  }
}
