package es.novaquality.spark

import faker._
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.time.LocalDate
import java.time.temporal.ChronoUnit.DAYS
import java.util.Properties
import scala.util.Random

object TransactionsSeeder extends App {

  val spark = configureSpark
  val JDBC_URL = "jdbc:postgresql://localhost:5432/postgres"
  val TABLE = "transactions"
  val connectionProperties = configurePostgres

  val data = createFakeData(1000)

  val fakeDataDF = spark.createDataFrame(data).toDF(
    "origin",
    "destination",
    "amount",
    "created_at",
    "message"
  )

  fakeDataDF.write
    .mode(SaveMode.Append)
    .jdbc(
      JDBC_URL,
      TABLE,
      connectionProperties
    )

  // Only for demo
  val readDataDF = spark.read.jdbc(
    JDBC_URL,
    TABLE,
    connectionProperties
  )

  println(s"Number of rows: ${readDataDF.count()}")

  private def configureSpark = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("TransactionsSeeder")
      .getOrCreate()

    spark
  }

  private def configurePostgres = {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("driver", "org.postgresql.Driver")
    connectionProperties.put("user", "postgres")
    connectionProperties.put("password", "12345")

    connectionProperties
  }

  private def createFakeData(numEntries: Int): Seq[(String, String, Float, LocalDate, String)] = {
    val random = new Random
    val fromDate = LocalDate.of(2023, 1, 1)
    val toDate = LocalDate.of(2023, 12, 31)
    var data = Seq.empty[(String, String, Float, LocalDate, String)]

    for (_ <- 1 to numEntries) {
      val origin: String = Address.state
      val destination: String = Address.state
      val amount: Float = random.between(0.00f, 100000.00f)
      val createdAt = randomDateBetween(fromDate, toDate)
      val message = Lorem.sentence(10)

      data = data :+ (
        origin,
        destination,
        amount,
        createdAt,
        message
      )
    }

    data
  }

  private def randomDateBetween(from: LocalDate, to: LocalDate): LocalDate = {
    val diff = DAYS.between(from, to)
    val random = new Random(System.nanoTime)
    from.plusDays(random.nextInt(diff.toInt))
  }
}
