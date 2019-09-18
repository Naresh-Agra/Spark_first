package Spark_First

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Spark_First {
  
  def main(arges:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val actual_data=sc.textFile("file:///E://Hadoop//Aditya Hadoop//Notes_3//Docs//txns")
    println("Actual Data")
    actual_data.foreach(println)
    
    val len_data = actual_data.filter(x=>x.length()>100)
    println("Length data >100")
    len_data.foreach(println)
    
      
    val quoted_data=len_data.filter(x=>x.contains("00"))
    println("Quoted Data")
    quoted_data.foreach(println)
   
    val flat_data=quoted_data.flatMap(x=>x.split(","))
    println("Flatten Data")
    flat_data.foreach(println)
    
  }
}