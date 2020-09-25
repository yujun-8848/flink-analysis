import java.net.URL

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._

object TableAPI {

  // 输入的登录事件样例类
  case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

  case class Student(id: Int, name: String, price: Long)

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val url: URL = getClass.getResource("/LoginLog.csv")
    val dataStream: DataStream[String] = env.readTextFile(url.getPath)
    val loginEventStream: DataStream[LoginEvent] = dataStream.map((data: String) => {
      val dataArrays: Array[String] = data.split(",")
      LoginEvent(dataArrays(0).trim.toLong, dataArrays(1).trim, dataArrays(2).trim, dataArrays(3).trim.toLong)
    })
    val tables: Table = tEnv.fromDataStream(loginEventStream)
    val table: Table = tables.select("eventType,ip")
      .filter("eventType == 'success'")
    table.toAppendStream[(String, String)].print()
    env.execute("TableAPI")

    tEnv.sqlQuery(
      """|
         |
         |""".stripMargin)


  }

}
