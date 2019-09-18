package Spark_Deployment1
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object Spark_Deployment1 {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("Deploy").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val data = sc.textFile("file:///E://Hadoop//Aditya Hadoop//Notes_3//Docs//txns")

    val fil_data=data.filter(x=>x.contains("Gymnastics"))
    println("Filtered Data")
    println
    fil_data.foreach(println)
    
    fil_data.saveAsTextFile("file:///E://Workouts//Executed_Results//Deploy2")
    
    
  }
  
}