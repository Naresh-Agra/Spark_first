package Spark_Deployment2
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark_Deployment2 {
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("Deploy").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val source = args(0)
    val dest = args(1)
    
    val data = sc.textFile(source)

    val fil_data=data.filter(x=>x.contains("Gymnastics"))
    println("Filtered Data")
    println
    fil_data.foreach(println)
    
    fil_data.saveAsTextFile(dest)
    
    
  }
}