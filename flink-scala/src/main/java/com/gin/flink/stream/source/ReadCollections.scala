package com.gin.flink.scala.stream.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

/**
  * @author gin
  *
  *         flink 读取本地集合
  */
object ReadCollections {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //读取本地集合一般不用
    val stream: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)

    stream.print()
    env.execute()

  }
}
