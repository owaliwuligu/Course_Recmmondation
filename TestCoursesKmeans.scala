import java.util
import java.util.LinkedList

import scala.io.Source
import scala.util.Random

object TestCoursesKmeans {

  def main(args: Array[String]):Unit= {
    val fileName = "E:/SparkTest/student_xg3.csv"
    val knumbers = 3
    var courses=new LinkedList[Point]

    //  读取文本数据
    val lines = Source.fromFile(fileName)

    for(line <- lines.getLines().toArray){
      val parts = line.split(",")

      var course=searchCourse(parts(1), courses)
      if(course!=null){
        course.scores.add(parts(2).toDouble)
      } else {
        var scores=new LinkedList[Double]
        scores.add(parts(2).toDouble)
        var c=new Point(parts(1), scores, 0, 0, 0, 0)
        courses.add(c)
      }
    }

    println(courses.size())
    var points=courses.toArray(new Array[Point](courses.size()))    //************

    //  随机初始化k个质心
    val centroids = new Array[Point](knumbers)
    for (i <- 0 until knumbers) {
      centroids(i) = points(new Random().nextInt(points.length))
    }
    val startTime = System.currentTimeMillis()
    println("initialize centroids:\n"+centroids.mkString("\n")+"\n")
    println("\n")
    //println("test points: \n" + points.mkString("\n") + "\n")

    val resultCentroids = ave_kmeans(points, centroids, 0.001)

    val newLink=new util.LinkedList[Double]()
    newLink.add(90.0)
    newLink.add(78.0)
    newLink.add(89.0)
    val forecastPoint=new Point(null, newLink,0, 0, 0, 0 )
    println("\nForecast Point(null, "+forecastPoint.getAve()+"):"+ave_forecastCourse(knumbers, resultCentroids, forecastPoint)+"\n")

    val endTime = System.currentTimeMillis()
    val runTime = endTime - startTime
    println("run Time: " + runTime + "\nFinal centroids: \n" + resultCentroids.mkString("\n"))
  }

  //  算法的核心函数
  def ave_kmeans(points: Seq[Point], centroids: Seq[Point], epsilon: Double): Seq[Point] = {
    //  最近质心为key值，将数据集分簇
    val clusters = points.groupBy(ave_closestCentroid(centroids, _))
    println("clusters: \n" + clusters.mkString("\n") + "\n")

    //  分别计算簇中数据集的平均数，得到每个簇的新质心
    val newCentroids = centroids.map(oldCentroid => {
      clusters.get(oldCentroid) match {
        case Some(pointsInCluster) => {
          println("nums of cluster:"+pointsInCluster.length)
          pointsInCluster.reduceLeft(_.aveAdd(_)).aveDev(pointsInCluster.length)
        }
        case None => oldCentroid
      }
    })

    //  计算新质心相对与旧质心的偏移量
    println("newCentroids"+newCentroids)
    println("centroids"+centroids)
    val movement = (centroids zip newCentroids).map({ case (a, b) => a.aveDistance(b)})
    println("Centroids changed by\n" + movement.map(d => "%3f".format(d)).mkString("(", ",", ")")
      + "\nto\n" + newCentroids.mkString(", ") + "\n")
    //  根据偏移值大小决定是否继续迭代，epsilon为最小偏移值
    if (movement.exists(_ > epsilon))
      ave_kmeans(points, newCentroids, epsilon)
    else
      return newCentroids
  }

  def ave_forecastCourse(knum:Int, centroids:Seq[Point], course:Point): String ={
    centroids.sortBy{c=>c.getAve()}
    println("centroids:"+centroids)
    if(knum==3){
      centroids(0).setCn("easy")
      centroids(1).setCn("middle")
      centroids(2).setCn("hard")
    }
    if(knum==5){
      centroids(0).setCn("very easy")
      centroids(1).setCn("easy")
      centroids(2).setCn("middle")
      centroids(3).setCn("hard")
      centroids(4).setCn("very hard")
    }

    var cenIndex=0
    var cenDis=10000.0
    var i=0
    for(cen <- centroids){
      val dis=course.aveDistance(cen)
      if(dis < cenDis){
        cenIndex=i
        cenDis=dis
      } else {
        i += 1
      }
    }

    centroids(cenIndex).cn
  }

  //  计算最近质心
  def ave_closestCentroid(centroids: Seq[Point], point: Point) = {
    centroids.reduceLeft((a, b) => if ((point.aveDistance(a)) < (point.aveDistance(b))) a else b)
  }

  // 寻找课程是否已存在
  def searchCourse(cno: String, courses: LinkedList[Point]):Point={    //判断某课程是否存在
    val points=courses.toArray(new Array[Point](courses.size()))

    for(p <- points){
      if(cno.trim.equals(p.cn.trim)){
        return p    //一定要加return
      }
    }
    null
  }
}

object Point {
  def random() = {
    new Point(null, null, Math.random(), Math.random(), Math.random(), Math.random())
  }
}

case class Point(var cn: String, val scores: LinkedList[Double], var ave:Double, var passP:Double,
                 var goodP: Double, var greatP:Double) {
  //def +(that: Point) = new Point(this.x + that.x, this.y + that.y)
  //def -(that: Point) = new Point(this.x - that.x, this.y - that.y)
  //def /(d: Double) = new Point(this.x / d, this.y / d)
  //def pointLength = math.sqrt(x * x + y * y)
  //def distance(that: Point) = (this - that).pointLength
  override def toString = "Point("+cn+","+getAve()+")"

  def aveAdd(that: Point)=new Point(null, null, this.getAve()+that.getAve(), 0, 0, 0)
  def aveDev(d: Double)=new Point(null, null, this.getAve()/d, 0, 0, 0)

  def aveDistance(that:Point): Double ={
    println("ave:"+this.getAve()+", "+that.getAve())
    math.sqrt((this.getAve()-that.getAve())*(this.getAve()-that.getAve()))
  }

  def setCn(cname:String): Unit ={
    this.cn=cname
  }

  def getAve(): Double ={
    var sum=0.0
    if(scores==null){
      return this.ave
    }
    val iter=this.scores.iterator()

    while(iter.hasNext){
      val scs=iter.next()
      if(scs > 0.0){
        sum += scs
      }
    }

    sum/scores.size()
  }

  def getPassPerc(): Double ={
    val iter=Iterator(scores)
    var num=0
    for(i<-iter){
      val score=i.getFirst
      if(score>=60)
        num+=1
    }

    num/scores.size()
  }

  def getGoodPerc(): Double ={
    val iter=Iterator(scores)
    var num=0
    for(i<-iter){
      val score=i.getFirst
      if(score>=80)
        num+=1
    }

    num/scores.size()
  }

  def getGreatPerc(): Double ={
    val iter=Iterator(scores)
    var num=0
    for(i<-iter){
      val score=i.getFirst
      if(score>=90)
        num+=1
    }

    num/scores.size()
  }
}
