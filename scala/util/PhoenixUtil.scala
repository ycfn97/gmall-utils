package util

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
 * Copyright(c) 2020-2021 sparrow All Rights Reserved
 * Project: gmall2020-parent
 * Package: util
 * ClassName: PhoenixUtil 
 *
 * @author 18729 created on date: 2020/12/5 0:05
 * @version 1.0
 * @since JDK 1.8
 */
object PhoenixUtil {
  def main(args: Array[String]): Unit = {
    val list:  List[ JSONObject] = queryList("select * from  GMALL2020_DAU")
    println(list)
  }

  def   queryList(sql:String):List[JSONObject]={
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val resultList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
    val conn: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop01,hadoop02,hadoop03:2181")
    val stat: Statement = conn.createStatement
    println(sql)
    val rs: ResultSet = stat.executeQuery(sql )
    val md: ResultSetMetaData = rs.getMetaData
    while (  rs.next ) {
      val rowData = new JSONObject();
      for (i  <-1 to md.getColumnCount  ) {
        rowData.put(md.getColumnName(i), rs.getObject(i))
      }
      resultList+=rowData
    }

    stat.close()
    conn.close()
    resultList.toList
  }
}
