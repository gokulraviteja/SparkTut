package com.grt.spark.tutorial.two
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ DoubleType, StructField, StructType}


object Main{


  def main(args: Array[String]): Unit = {
    println("Hello Spark 2 With SBT")

    // Dataframes - set of rows.
    // operations resemble sql
    // Do optimizations  on its own

    val spark=SparkSession.builder().master("local[*]").appName("Spark Sql").getOrCreate()

    println("Spark session set")
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    val data=spark.read.csv("MN212142_9392.csv")

    data.schema.printTreeString()

    val schema =StructType(Array(
      StructField("sid", DoubleType)
    ))

    //val data1=spark.read.schema(schema).csv("MN212142_9392.csv")
    // If to provide a schema
    // useful methods .limit .drop('colname') .describe() .createDataFrame().cache() .collect() which pulls to local machine

    data.show(5)
    data.filter($"_c0"<3).show(3)
    data.filter('_c0<2).show(2)


    data.createOrReplaceTempView("data")
    val puresql =spark.sql("""  select * from data """).show(3)
    spark.stop()


  }

}