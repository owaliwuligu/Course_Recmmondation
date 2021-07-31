import scala.collection.JavaConverters._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object Kmeans {
  def main(args : Array[String]){
    val conf = new SparkConf()
      .setAppName("map")
      .setMaster("local")
    val sc=new SparkContext(conf)
  println("test4")
    /***读文件
    val lines=sc.textFile("E:/Spark/data4.txt")
    println("test3")
    val parsedTrainingData = lines.filter(!isColumnNameLine(_)).map(line => {
      Vectors.dense(line.split("\t").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
    }).cache()

      ***/
    val db = new SDBProcess()
    val rawTrainData = db.getKmeansTrainData()
    val parsedTrainingData = rawTrainData.map(line=>{
      val line_array:Array[Double] = Array[Double](line._1,line._2,line._3,line._4)
      Vectors.dense(line_array)
    }).cache()
    parsedTrainingData.collect().foreach(line => println(line))
    // Cluster the data into two classes using KMeans

    val numClusters = 4
    val numIterations = 30
    val runTimes = 10
    var clusterIndex:Int = 0
    println("test2")
    val clusters:KMeansModel = KMeans.train(parsedTrainingData, numClusters, numIterations,runTimes)
    println("Cluster Number:" + clusters.clusterCenters.length)
    println("Cluster Centers Information Overview:")
    clusters.clusterCenters.foreach( x => {
      println("Center Point of Cluster " + clusterIndex + ":")
      println(x)
      clusterIndex += 1
    })
    println("test1")
    //begin to check which cluster each test data belongs to based on the clustering result
    /*** 读文件***
    val rawTestData = sc.textFile("E:/Spark/data4.txt")
    val parsedTestData = rawTestData.filter(!isColumnNameLine(_)).map(line =>
    {
      Vectors.dense(line.split("\t").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
    })
      ***/

    val rawTestData = db.getKmeansTestData()
    val parsedTestData = rawTestData.map(line =>
    {
      val line_array:Array[Double] = Array[Double](line._2._1,line._2._2,line._2._3,line._2._4)
      (line._1,Vectors.dense(line_array))
    })


   val kmeans_result= parsedTestData.map(testDataLine => {
      val predictedClusterIndex:Int = clusters.predict(testDataLine._2)
      println("The course " + testDataLine._1 + " belongs to cluster " + predictedClusterIndex)
      (testDataLine._1,predictedClusterIndex.toString)
    })
    kmeans_result.foreach(f=>{
      println(f)
    })
    db.saveKmeans(kmeans_result)
    println("Spark MLlib K-means clustering test finished.")
  }

}
