import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class CustomQueryExecutionListener extends QueryExecutionListener {
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    println(s"Query succeeded: $funcName, took ${durationNs / 1e6} ms")
    println(qe.toString)
    println("Generated code:")
    println(qe.executedPlan.toString)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    println(s"Query failed: $funcName, exception: ${exception.getMessage}")
    println(qe.toString)
    println("Generated code:")
    println(qe.executedPlan.toString)
  }
}