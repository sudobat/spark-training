package es.novaquality.spark

import faker._
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.time.LocalDate
import java.time.temporal.ChronoUnit.DAYS
import java.util.Properties
import scala.util.Random

object PostgresSeeder extends App {

  val spark = configureSpark
  val JDBC_URL = "jdbc:postgresql://localhost:5432/postgres"
  val TRANSACTIONS_TABLE = "transactions"
  val PROBLEMS_TABLE = "problems"
  val connectionProperties = configurePostgres

  val transactionsData = createTransactionsFakeData(1000)

  val transactionsDF = spark.createDataFrame(transactionsData).toDF(
    "origin",
    "destination",
    "amount",
    "created_at",
    "message"
  )

  val problemsData = createProblemsFakeData(5000)

  val problemsDF = spark.createDataFrame(problemsData).toDF(
    "origin",
    "created_at",
    "message"
  )

  transactionsDF.write
    .mode(SaveMode.Overwrite)
    .jdbc(
      JDBC_URL,
      TRANSACTIONS_TABLE,
      connectionProperties
    )

  problemsDF.write
    .mode(SaveMode.Overwrite)
    .jdbc(
      JDBC_URL,
      PROBLEMS_TABLE,
      connectionProperties
    )

  private def configureSpark = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("PostgresSeeder")
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

  private def createTransactionsFakeData(numEntries: Int): Seq[(String, String, Float, LocalDate, String)] = {
    val random = new Random
    val fromDate = LocalDate.of(2023, 12, 1)
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

  private def createProblemsFakeData(numEntries: Int): Seq[(String, LocalDate, String)] = {
    val random = new Random
    val fromDate = LocalDate.of(2023, 12, 1)
    val toDate = LocalDate.of(2023, 12, 31)
    var data = Seq.empty[(String, LocalDate, String)]

    for (_ <- 1 to numEntries) {
      val origin: String = Address.state
      val createdAt = randomDateBetween(fromDate, toDate)
      val message = Lorem.sentence(10)

      data = data :+ (
        origin,
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
