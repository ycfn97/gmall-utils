package util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core._

/**
 * Copyright(c) 2020-2021 sparrow All Rights Reserved
 * Project: gmall2020-parent
 * Package: util
 * ClassName: MyEsUtil 
 *
 * @author 18729 created on date: 2020/11/27 18:56
 * @version 1.0
 * @since JDK 1.8
 */
object MyEsUtil {
  var jestClientFactory:JestClientFactory=null;

  def build() = {
    jestClientFactory = new JestClientFactory
    jestClientFactory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop01:9200")
    .multiThreaded(true).maxTotalConnection(20)
    .connTimeout(10000).readTimeout(10000).build())
  }

  def getClient:JestClient={
    if(jestClientFactory==null)
      build()
      jestClientFactory.getObject
  }


  def addDoc(): Unit = {
    val client = getClient
    val index = new Index.Builder(Movie0105("0104", "龙岭迷窟", "鬼吹灯")).index("movie0105_test_20200619")
      .`type`("_doc").id("0104").build();
    val message = client.execute(index).getErrorMessage
    if(message!=null) print(message)
    client.close()
  }

  def bulkDoc(sourceList:List[(String,Any)],indexName:String)={
    if (sourceList!=null&&sourceList.size>0){
      val client = getClient
      val builder = new Bulk.Builder //批次操作
      for ((id,source) <- sourceList ) {
        val index = new Index.Builder(source).index(indexName).`type`("_doc").id(id).build()
        builder.addAction(index)
      }
      val bulk: Bulk = builder.build()
      val result: BulkResult = client.execute(bulk)
      val items: util.List[BulkResult#BulkResultItem] = result.getItems
      println("保存到ES:"+items.size()+"条数")
      client.close()
    }
  }

  def main(args: Array[String]): Unit = {
    addDoc();
  }

  case class Movie0105 (id:String ,movie_name:String,name:String);
}
