package com.grt.spark.tutorial.one

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.Ordering

object Main{


  def main(args: Array[String]): Unit = {
    println("Hello Spark With MAVEN")
    val conf=new SparkConf().setAppName("Temp Data").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val lines=sc.textFile("MN212142_9392.csv").filter(!_.contains("Day"))     // lines is an rdd of strings
    //val lines=sc.textFile("file:///home/bellapukondar1/MN212142_9392.csv").filter(!_.contains("Day"))
    //this file:/// should be used when running in an hadoop ecosystem and file to be read from local
    //and not from hadoop else it will search for the file in hdfs://blahblahblah/user/bellapukondar1/sample.csv something like that.


    lines.take(5).foreach(println)
    var data = lines.flatMap { line =>
      val p = line.split(",")
      if (p(7) == "." || p(8) == "." || p(9) == ".") Seq.empty else
        Seq( TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt, TempData.toDoubleOrNeg(p(5)), TempData.toDoubleOrNeg(p(6)), p(7).toDouble, p(8).toDouble, p(9).toDouble))
    }
    //data is an rdd of tempdata's


    data.take(5).foreach(println)
    val maxtemp= data.map(_.tmax).max()
    val hotdays=data.filter( _.tmax==maxtemp )
    print(hotdays.collect().mkString(", "))
    println(data.max()(Ordering.by(_.tmax)))
    println(data.reduce((td1,td2)=> if(td1.tmax>=td2.tmax) td1 else  td2))


    // Not efficient (as lazy) the data rdd  for every action will be created in memory .
    // As data rdd is used more , it should be present persisted in memory , so we do cache it .


    data = lines.flatMap { line =>
      val p = line.split(",")
      if (p(7) == "." || p(8) == "." || p(9) == ".") Seq.empty else
        Seq( TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt, TempData.toDoubleOrNeg(p(5)), TempData.toDoubleOrNeg(p(6)), p(7).toDouble, p(8).toDouble, p(9).toDouble))
    }.cache()

    println(data.max()(Ordering.by(_.tmax)))
    println(data.reduce((td1,td2)=> if(td1.tmax>=td2.tmax) td1 else  td2))

    //DoubleRDD Func...
    println(data.map(_.tmax).stdev(),"Standard Dev Tmax")
    println(data.map(_.day).stdev(),"Standard Dev Day")





  }

}