package es.novaquality.spark

import org.apache.spark.sql.SparkSession

import java.util.Properties

object PostgresReader extends App {

  val spark = configureSpark
  val JDBC_URL = "jdbc:postgresql://localhost:5432/postgres"
  val TRANSACTIONS_TABLE = "transactions"
  val PROBLEMS_TABLE = "problems"
  val connectionProperties = configurePostgres

  val transactionsDF = spark.read.jdbc(
    JDBC_URL,
    TRANSACTIONS_TABLE,
    connectionProperties
  )

  val problemsDF = spark.read.jdbc(
    JDBC_URL,
    PROBLEMS_TABLE,
    connectionProperties
  )

  transactionsDF.printSchema()
  problemsDF.printSchema()

  private def configureSpark = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("PostgresReader")
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
}
