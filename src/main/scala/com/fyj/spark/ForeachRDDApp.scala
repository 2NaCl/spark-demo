package com.fyj.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用SparkStreaming完成有状态统计，并将结果写入mysql
  */
object ForeachRDDApp {
  def main(args: Array[String]): Unit = {

    //基本配置代码
    var sc = new SparkConf().setAppName("StatefulWordCount").setMaster("local[*]")
    var ssc = new StreamingContext(sc, Seconds(5))

    //如果使用了带状态的stateful的算子，就一定要设置checkpoint，既然我们要做的是新值追加老值的操作，所以需要将老值存放起来才能追加
    //因为这里是测试环境， 如果是生产环境就要把目录设置到HDFS文件夹
    ssc.checkpoint(".")

    //业务代码,还是使用socket的方式
    val lines = ssc.socketTextStream("192.168.243.20",6789)
    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    //state.print()
    //写入mysql
//    result.foreachRDD(rdd=>{
//      val connection = createConnection() //executed at the driver
//      rdd.foreach{ record =>
//        var sql = "insert into wordcount(word,wordcount) values('"+record._1+"',"+record._2+")"
//        connection.createStatement().execute(sql)
//      }
//    })
    result.foreachRDD(rdd=> {

      rdd.foreachPartition(partitionOfRecords => {

        val connection = createConnection()
        partitionOfRecords.foreach(record => {
          var sql = "insert into wordcountdemo(word,wordcount) values('" + record._1 + "'," + record._2 + ")"
          connection.createStatement().execute(sql)
        })
        connection.close()
      })
  })

      ssc.start()
      ssc.awaitTermination()

    }
  /**
    * 连接数据库
    */
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/wordcount","root","")
  }
}
