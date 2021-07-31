import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import util.control.Breaks
import util.control.Breaks._

object anonymy {
  def main(args: Array[String]): Unit =
  {
    val conf = new SparkConf()
      .setAppName("map")
      .setMaster("local")
    val sc=new SparkContext(conf)
    //读取原数据
    val lines=sc.textFile("E:/Spark/anonymy_input1.txt")
    val col = 10
    val anonymy_Data:Array[Array[Double]] = new Array[Array[Double]](col)
    var len_Array:Int = 0
    for(i <- 0 to col-1)
      {
        anonymy_Data(i) = new Array[Double](150)
      }
    //取第一列成绩
    for(line_id <- 0 to col-1) {
      val line_Data = lines.filter(!isColumnNameLine(_)).map(line => {
        line.split("\t")(line_id + 1).toDouble
      })
      line_Data.collect().foreach(line => {
        println(line)
      })
      //将元素按照属性分类(优秀、良好、及格、不及格),且将元素转化成一维向量，以便后续聚类
      val trainData: Array[RDD[org.apache.spark.mllib.linalg.Vector]] = new Array[RDD[org.apache.spark.mllib.linalg.Vector]](5)
      val SegData1 = line_Data.filter(_ < 60.0)
      trainData(0) = SegData1.map(line => {
        Vectors.dense(line)
      })

      val SegData2 = line_Data.filter(_ >= 60.0).filter(_ < 70.0)
      //SegData2.collect().foreach(line=>println(line))
      trainData(1) = SegData2.map(line => {
        Vectors.dense(line)
      })

      val SegData3 = line_Data.filter(_ >= 70.0).filter(_ < 80.0)
      trainData(2) = SegData3.map(line => {
        Vectors.dense(line)
      })

      val SegData4 = line_Data.filter(_ >= 80.0).filter(_ < 90.0)
      trainData(3) = SegData4.map(line => {
        Vectors.dense(line)
      })

      val SegData5 = line_Data.filter(_ >= 90.0).filter(_ < 100.0)
      trainData(4) = SegData5.map(line => {
        Vectors.dense(line)
      })

      val preData: Array[RDD[org.apache.spark.mllib.linalg.Vector]] = new Array[RDD[org.apache.spark.mllib.linalg.Vector]](5)
      preData(0) = trainData(0)
      preData(1) = trainData(1)
      preData(2) = trainData(2)
      preData(3) = trainData(3)
      preData(4) = trainData(4)
      val cluster: Array[KMeansModel] = new Array[KMeansModel](5)
      var numClusters = 3
      val numIterations = 30
      val runTimes = 10
      var clusterIndex: Int = 0
      var flag = 1
      //各级元素聚类
      for (tid <- 0 to 4) {
        //println("tid:"+tid)
        //println("RDDcount:"+trainData(tid).count())
        breakable {
          if (trainData(tid).count() == 0)
            break

          println("not continue")
          numClusters = 3
          flag = 1
          clusterIndex = 0
          while (numClusters > 0 && flag == 1) {
            //println(numClusters)
            flag = 0
            clusterIndex = 0
            cluster(tid) = KMeans.train(trainData(tid), numClusters, numIterations, runTimes)
            //println("test")
            println("Cluster Number:" + cluster(tid).clusterCenters.length)
            println("Cluster Centers Information Overview:")
            cluster(tid).clusterCenters.foreach(x => {
              var count: Array[Int] = new Array[Int](10)
              for (id <- 0 to 9)
                count(id) = 0
              trainData(tid).collect().foreach(testDataLine => {
                val predictedClusterIndex: Int = cluster(tid).predict(testDataLine)
                count(predictedClusterIndex) = count(predictedClusterIndex) + 1
              })
              //for(id <- 0 to clusters1.clusterCenters.length)
              //  println(id+": "+count(id))

              for (id <- 0 to cluster(tid).clusterCenters.length - 1) {
                if (count(id) < 2) {
                  flag = 1
                  Breaks
                }
              }
              println("Center Point of Cluster " + clusterIndex + ":")
              println(x)
              clusterIndex += 1
            })
            if (flag == 1)
              numClusters = numClusters - 1
          }
        }
      }
      //输出各个阶段的元素及其聚类后属于的中心
      for (id <- 0 to 4) {
        breakable{
          if(preData(id).count()==0)
            break
        }
        preData(id).collect().foreach(testDataLine => {
          val predictedClusterIndex: Int = cluster(id).predict(testDataLine)
          println("The data " + testDataLine.toString + " belongs to cluster " + predictedClusterIndex + " " + cluster(id).clusterCenters(predictedClusterIndex))
        })
        println("####################################################################")
        println("####################################################################")
      }
      //该数组存放第一列数匿名后的结果
      //     val anonymy_Array1: Array[Double] = new Array[Double](100)
      var len_Array1 = 0
      line_Data.collect().foreach(line => {
        if (line < 60) {
          val temp_vector = Vectors.dense(line)
          val predictedClusterIndex: Int = cluster(0).predict(temp_vector)
          //val temp = cluster(0).clusterCenters(predictedClusterIndex).toArray.toList
          anonymy_Data(line_id)(len_Array1) = cluster(0).clusterCenters(predictedClusterIndex).toArray.toList(0)
          len_Array1 += 1
        }
        else if (line >= 60 && line < 70) {
          val temp_vector = Vectors.dense(line)
          val predictedClusterIndex: Int = cluster(1).predict(temp_vector)
          //val temp = cluster(0).clusterCenters(predictedClusterIndex).toArray.toList
          anonymy_Data(line_id)(len_Array1) = cluster(1).clusterCenters(predictedClusterIndex).toArray.toList(0)
          len_Array1 += 1
        }
        else if (line >= 70 && line < 80) {
          val temp_vector = Vectors.dense(line)
          val predictedClusterIndex: Int = cluster(2).predict(temp_vector)
          //val temp = cluster(0).clusterCenters(predictedClusterIndex).toArray.toList
          anonymy_Data(line_id)(len_Array1) = cluster(2).clusterCenters(predictedClusterIndex).toArray.toList(0)
          len_Array1 += 1
        }
        else if (line >= 80 && line < 90) {
          val temp_vector = Vectors.dense(line)
          val predictedClusterIndex: Int = cluster(3).predict(temp_vector)
          //val temp = cluster(0).clusterCenters(predictedClusterIndex).toArray.toList
          anonymy_Data(line_id)(len_Array1) = cluster(3).clusterCenters(predictedClusterIndex).toArray.toList(0)
          len_Array1 += 1
        }
        else {
          val temp_vector = Vectors.dense(line)
          val predictedClusterIndex: Int = cluster(4).predict(temp_vector)
          //val temp = cluster(0).clusterCenters(predictedClusterIndex).toArray.toList
          anonymy_Data(line_id)(len_Array1) = cluster(4).clusterCenters(predictedClusterIndex).toArray.toList(0)
          len_Array1 += 1
        }
        len_Array = len_Array1
      })
    }
    println("##########################################################")
    println("##########################################################")
    println("##########################################################")
    for (id <- 0 to len_Array - 1) {
      for (col <- 0 to col-1) {
        val temp = anonymy_Data(col)(id)
        print(f"$temp%1.3f" + "\t")
      }
      println()
    }
    println("##########################################################")
    println("##########################################################")
    println("##########################################################")
    var i = 1
    val result_Data:Array[String] = new Array[String](len_Array)
    for(id<-0 to len_Array-1){
      result_Data(id) = ""
    }
    for(id<-0 to len_Array-1) {
          for(id2 <- 0 to col-1){
            var temp = anonymy_Data(id2)(id)
            result_Data(id) += f"$temp%1.3f"
            result_Data(id) += "\t"
            }
    }
    for(id<-0 to len_Array-1){
      println(result_Data(id))
    }
    val result = sc.parallelize(result_Data)
    result.foreach(x=>println(x))

    //result.saveAsTextFile("E:/Spark/ouput_score.txt")
  }

  private def isColumnNameLine(line:String):Boolean = {
    if (line != null &&
      line.contains("ID")) true
    else false
  }
}
