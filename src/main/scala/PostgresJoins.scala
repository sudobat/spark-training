package es.novaquality.spark

import SparkLogLevels.setLogLevelToWarn

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, desc}
import java.util.Properties

object PostgresJoins extends App {

  setLogLevelToWarn()

  val sparkSession = configureSpark
  val JDBC_URL = "jdbc:postgresql://localhost:5432/postgres"
  val TRANSACTIONS_TABLE = "transactions"
  val PROBLEMS_TABLE = "problems"
  val connectionProperties = configurePostgres

  val transactionsDF = sparkSession.read.jdbc(
    JDBC_URL,
    TRANSACTIONS_TABLE,
    connectionProperties
  ).toDF(
    "origin",
    "destination",
    "amount",
    "transaction_date",
    "transaction_concept"
  )

  val problemsDF = sparkSession.read.jdbc(
    JDBC_URL,
    PROBLEMS_TABLE,
    connectionProperties
  ).toDF(
    "origin",
    "problem_date",
    "problem_description"
  )

  println("Transactions")
  transactionsDF.printSchema()

  println("Problems")
  problemsDF.printSchema()

//  innerJoinByOrigin()
//  innerJoinByOriginAndDate()

  val totalAmountOfTransactionsByOriginAndDateDF = transactionsDF
    .groupBy("origin", "transaction_date")
    .sum("amount")

  val totalProblemsByOriginAndDateDF = problemsDF
    .groupBy("origin", "problem_date")
    .count()

//  totalAmountOfTransactionsAndProblemsByOriginAndDate()
//  totalAmountOfTransactionsWithoutProblemsByOriginAndDate()
//  crossJoinExample()

  private def innerJoinByOrigin(): Unit = {
    println("Inner Join by origin")
    val innerJoinByOriginDF = transactionsDF.join(
      problemsDF,
      Seq("origin"),
      "inner"
    )
    innerJoinByOriginDF.show()
  }
  private def innerJoinByOriginAndDate(): Unit = {
    println("Inner Join by origin and date")
    val innerJoinByOriginAndDateDF = transactionsDF.join(
      problemsDF,
      transactionsDF("origin") === problemsDF("origin")
        and transactionsDF("transaction_date") === problemsDF("problem_date"),
      "inner"
    )
    innerJoinByOriginAndDateDF.show()
  }
  private def totalAmountOfTransactionsAndProblemsByOriginAndDate(): Unit = {
    println("Importe total con número de problemas y índice de riesgo por día y origen")

    val totalAmountOfTransactionsAndProblemsByOriginAndDateDF = totalAmountOfTransactionsByOriginAndDateDF
      .join(
        totalProblemsByOriginAndDateDF,
        totalAmountOfTransactionsByOriginAndDateDF("origin") === totalProblemsByOriginAndDateDF("origin")
          and totalAmountOfTransactionsByOriginAndDateDF("transaction_date") === totalProblemsByOriginAndDateDF("problem_date"),
        "inner"
      ).select(
      totalAmountOfTransactionsByOriginAndDateDF("origin"),
      totalAmountOfTransactionsByOriginAndDateDF("transaction_date"),
      totalAmountOfTransactionsByOriginAndDateDF("sum(amount)").as("total_amount"),
      totalProblemsByOriginAndDateDF("count").as("num_problems")
    ).withColumn("risk_index", col("total_amount") * col("num_problems"))
      .orderBy(desc("transaction_date"), desc("risk_index"))

    totalAmountOfTransactionsAndProblemsByOriginAndDateDF.show()
  }
  private def totalAmountOfTransactionsWithoutProblemsByOriginAndDate(): Unit = {
    println("Importe total sin problemas por día y origen")

    val totalAmountOfTransactionsWithoutProblemsByOriginAndDateDF = totalAmountOfTransactionsByOriginAndDateDF
      .join(
        totalProblemsByOriginAndDateDF,
        totalAmountOfTransactionsByOriginAndDateDF("origin") === totalProblemsByOriginAndDateDF("origin")
          and totalAmountOfTransactionsByOriginAndDateDF("transaction_date") === totalProblemsByOriginAndDateDF("problem_date"),
        "left_anti"
      ).orderBy(desc("transaction_date"), desc("sum(amount)"))

    totalAmountOfTransactionsWithoutProblemsByOriginAndDateDF.show()
  }
  private def crossJoinExample(): Unit = {
    val differentOriginsDF = totalAmountOfTransactionsByOriginAndDateDF
      .select("origin")
      .limit(5)

    differentOriginsDF.show()

    val differentDatesDF = totalAmountOfTransactionsByOriginAndDateDF
      .select("transaction_date")
      .limit(5)

    differentDatesDF.show()

    val crossJoinDF = differentOriginsDF.crossJoin(
      differentDatesDF
    )

    crossJoinDF.show()
  }

  private def configureSpark = {
    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("PostgresJoins")
      .getOrCreate()

    sparkSession
  }

  private def configurePostgres = {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("driver", "org.postgresql.Driver")
    connectionProperties.put("user", "postgres")
    connectionProperties.put("password", "12345")

    connectionProperties
  }
}
