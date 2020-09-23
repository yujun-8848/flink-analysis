import java.net.URL

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment

object TableAPI {

  case class Student(id: Int, name: String, price: Long)

  def main(args: Array[String]): Unit = {

    //创建流处理table环境
    val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inBatchMode().build()
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val url: URL = getClass.getResource("/student.txt")
    val tEnv: TableEnvironment = BatchTableEnvironment.create(env)
    val dataSet: DataSet[Student] = env.readTextFile(url.getPath)
      .map(data => {
        val strings: Array[String] = data.split(",")
        Student(strings(0).toInt, strings(1), strings(2).toLong)
      })

    val table: Table = tEnv.from(url.getPath)
    val result: TableResult = tEnv.executeSql("select * from table")
    result.print()

  }

}
