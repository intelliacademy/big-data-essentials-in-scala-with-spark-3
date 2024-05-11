package com.intellibucket.lessons

import java.sql.{Connection, DriverManager}

case class GConnection(url:String,username:String,password:String){
  def from() : Connection =
    DriverManager.getConnection(url,username,password)
}

case object GConnection{
  def apply() : Connection = new GConnection("jdbc:postgresql://localhost:5432/postgres","docker","docker").from()
  def as() : GConnection = new GConnection("jdbc:postgresql://localhost:5432/postgres","docker","docker")
}

object ConnectionProvider {
  var connectionPool: List[Connection] = List()
  def newConnection(): Connection = GConnection()
}
