package com.gin.flink.scala.stream.source

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import scala.util.Random

//自定义一个支持多并行读的数据源
object CustomSourceParallel {
  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //[String] 发射数据类型
    val stream = env.addSource(new ParallelSourceFunction[String] {
      var flag = true

      //发射数据
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        //run  读取任何地方数据，然后将数据 发射出去  redis
        val random = new Random()
        while (flag) {
          ctx.collect("hello" + random.nextInt(1000))
          Thread.sleep(500)
        }
      }
      //停止
      override def cancel(): Unit = {
        flag = false
      }
    }).setParallelism(2)
    stream.print().setParallelism(2)
    env.execute()


  }
}
