import java.net.URL

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object OrderTimeoutWithoutComplex {


  val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取订单数据
    val resource: URL = getClass.getResource("/OrderLog.csv")
    //    val orderEventStream = env.readTextFile(resource.getPath)
    val orderEventStream: KeyedStream[OrderEvent, Long] = env.socketTextStream("localhost", 7777)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    // 定义process function进行超时检测
    //    val timeoutWarningStream = orderEventStream.process( new OrderTimeoutWarning() )
    val orderResultStream: DataStream[OrderResult] = orderEventStream.process(new OrderPayMatch())

    orderResultStream.print("payed")
    orderResultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout without cep job")
  }

  class OrderPayMatch extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed", classOf[Boolean]))

    lazy val timeState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time-state", classOf[Long]))

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {

      // 根据状态的值，判断哪个数据没来
      if (isPayedState.value()) {
        // 如果为true，表示pay先到了，没等到create
        ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "already payed but not found create log"))
      } else {
        // 表示create到了，没等到pay
        ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
      }
      isPayedState.clear()
      timeState.clear()
    }

    override def processElement(i: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, collector: Collector[OrderResult]): Unit = {
      val ispayed: Boolean = isPayedState.value()
      val timeTs: Long = timeState.value()
      if (i.eventType == "create") {
        if (ispayed) {
          collector.collect(OrderResult(i.orderId, "payed successfully"))
          context.timerService().deleteEventTimeTimer(timeTs)
          isPayedState.clear()
          timeState.clear()
        } else {
          val ts: Long = i.eventTime * 1000L + 15 * 60 * 1000L
          context.timerService().registerEventTimeTimer(ts)
          timeState.update(ts)
        }
      } else if (i.eventType == "pay") {
        if (timeTs > 0) {
          if (timeTs > i.eventTime * 1000) {
            collector.collect(OrderResult(i.orderId, "payed successfully"))
          } else {
            context.output(orderTimeoutOutputTag, OrderResult(i.orderId, "payed but already timeout"))
          }
          context.timerService().deleteEventTimeTimer(timeTs)
          isPayedState.clear()
          timeState.clear()
        } else {
          context.timerService().registerEventTimeTimer(i.eventTime * 1000L)
          isPayedState.update(true)
          timeState.update(i.eventTime * 1000L)
        }
      }
    }
  }

}


