package com.jd.kafka

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object TestCounter {


  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val data = env.fromElements("a", "b", "c", "d")

    val res = data.map(new RichMapFunction[String, String] {
      val numLines = new IntCounter() //定义
      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        getRuntimeContext.addAccumulator("num-lines", numLines) // 注册
      }

      override def map(in: String): String = {
        this.numLines.add(1) // 使用
        in
      }
    }).setParallelism(4)
    res.writeAsText("d:/sshdata/count").setParallelism(1)
    val jobResult = env.execute("BatchDemoCounter")
    val num = jobResult.getAccumulatorResult[Int]("num-lines") // 获取
    println(num)
  }
}

