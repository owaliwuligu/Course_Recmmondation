import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source
object Lwy_01 {
  def main (args:Array[String]){

    val conf = new SparkConf().setAppName("FPGrowthTest").setMaster("local")
    val sc = new SparkContext(conf)

    val anonymy = sc.textFile("E:/Spark/output_score.txt/part-00000.txt")
    anonymy.foreach(x => println(x))
  }
}
