import java.net.URL
import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

// 定义输入订单事件的样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

// 定义输出结果样例类
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 1. 读取订单数据
    val resource: URL = getClass.getResource("/OrderLog.csv")
    val orderEventStream: KeyedStream[OrderEvent, Long] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    val timeData: Pattern[OrderEvent, OrderEvent] =
      Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
        .followedBy("follow").where(_.eventType == "pay")
        .within(Time.minutes(15))
    val outputLag = new OutputTag[OrderResult]("timeoutID")

    val cep: PatternStream[OrderEvent] = CEP.pattern(orderEventStream, timeData)
    val value: DataStream[OrderResult] = cep
      .select(outputLag, new timeoutEnvent(), new selectEnvent())
    value.print("payed")
    value.getSideOutput(outputLag).print("timeout")
    env.execute()
  }

}

class timeoutEnvent extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
    val tiemoutID: Long = pattern.get("begin").iterator().next().orderId
    val timeoutSteamp: Long = pattern.get("begin").iterator().next().eventTime
    OrderResult(tiemoutID, "超时支付 " + "" + timeoutSteamp.toString)

  }
}

class selectEnvent extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payID: Long = pattern.get("follow").iterator().next().orderId
    OrderResult(payID, "成功支付")

  }
}
