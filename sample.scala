import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row


val url = sc.broadcast("jdbc:postgresql://localhost:5433/postgres")
val driver = sc.broadcast("org.postgresql.Driver")
val user = sc.broadcast("root")
val password = sc.broadcast("123")
val orderDF = spark
	.read
	.format("jdbc")
	.option("driver", driver.value)
	.option("url", url.value)
	.option("user", user.value)
	.option("password", password.value)
	.option("dbtable", "public.orders")
	.load
	.where("updated_at between '2023-01-03 00:00:00' and '2023-01-03 23:59:59')
        .where("final_state = 'paid')


def writeRowsToPostgres(rows: Iterator[Row], conn: Connection): Unit = {
  val stmt = conn.prepareStatement("INSERT INTO public.rfm_metrics (customer_id, recency, frequency, monetary) VALUES (?, ?, ?, ?) ON CONFLICT (customer_id) DO UPDATE SET recency = case when excluded.recency > rfm_metrics.recency then excluded.recency else rfm_metrics.recency end, frequency = rfm_metrics.frequency + excluded.frequency, monetary = rfm_metrics.monetary + excluded.monetary")
  for (row <- rows) {
    stmt.setLong(1, row.getLong(0))
    stmt.setTimestamp(2, row.getTimestamp(1))
    stmt.setInt(3, row.getInt(2))
    stmt.setLong(4, row.getLong(3))
    stmt.addBatch()
  }
  stmt.executeBatch()
}

def writePartitionToPostgres(rows: Iterator[Row]): Unit = {

  Class.forName(driver.value)
  val conn = DriverManager.getConnection(url.value, user.value, password.value)
  writeRowsToPostgres(rows, conn)
  conn.close()
}


val result = orderDF
	.groupBy("customer_id")
	.agg(expr("max(created_at)").as("recency"),expr("count(id)").cast("int").as("frequency"),expr("sum(total_amount)").as("monetary"))
	.foreachPartition((r: Iterator[Row]) => writePartitionToPostgres(r))

