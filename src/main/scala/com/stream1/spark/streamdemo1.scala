package com.stream1.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j._


object streamdemo1 {
  def main(args:Array[String]): Unit= {
    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir","C:\\hadoop")

    val spark = SparkSession.builder().master("local[2]").appName("demo").getOrCreate()

    val schema = StructType(List(StructField("date", TimestampType, true), StructField("message", StringType, true)))

    val streamdf = spark.readStream.option("delimiter", "|").schema(schema).csv("C:\\Users\\yjasw\\PycharmProjects\\Scala-learnings\\untitled3\\src\\main\\Logs")

    streamdf.createOrReplaceTempView("streamdf1")

    val outdf = spark.sql("select date,count(message) from streamdf1 group by date")

    outdf.writeStream.format("console").outputMode("update").start().awaitTermination()

  }


}
