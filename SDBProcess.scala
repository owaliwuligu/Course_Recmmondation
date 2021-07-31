import java.util.Properties

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.sql.types._
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Date
import java.io.FileWriter

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.rdd.RDD

class SDBProcess extends Serializable{
  val spark= SparkSession.builder().appName("SQLIPLocation").master("local").getOrCreate()

  def getTrainData():RDD[(String,String,Double)]={
    val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/data2")  //*****这是数据库名
      .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "student_rg")//*****是表名
      .option("user", "root").option("password", "").load()
    val temp_rdd = jdbcDF.rdd
    val result_rdd = temp_rdd.map(f=>(f.getString(0),f.getString(1),f.getString(4),f.getDouble(2)))
    val result_rdd2 = result_rdd.filter(_._4<70)
    val result_rdd3 = result_rdd2.filter(!_._3.equals("2016级"))
    val result_rdd4 = result_rdd3.map(f=>(f._1, f._2, f._4))
    result_rdd4
  }

  def getPreviewData():RDD[(String, String, Double)]={
    val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/data2")  //*****这是数据库名
      .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "student_rg")//*****是表名
      .option("user", "root").option("password", "").load()
    val temp_rdd = jdbcDF.rdd
    val result_rdd = temp_rdd.map(f=>(f.getString(0), f.getString(1), f.getString(4), f.getDouble(2), f.getString(3)))
    val result_rdd2 = result_rdd.filter(_._4<65.0)
    val result_rdd3 = result_rdd2.filter(_._3.equals("2016级"))
    //val result_rdd4_ = result_rdd3.filter(_._5.equals("2016秋"))
    //val result_rdd4 = result_rdd4_.map(f=>(f._1, f._2, f._4))
    val result_rdd4 = result_rdd3.map(f=>(f._1, f._2, f._4))
    result_rdd4
  }

  def trainData(rdd:RDD[(String, String, Double)]):RDD[(String, String, Double)]={
    val Sr = new Srecommend()
    val rdd_perf = rdd.map(f=>(f._1, f._2, 70-f._3))
    val cosine_similarity_rdd = Sr.CosineSimilarity(rdd_perf)
   // cosine_similarity_rdd.foreach(f=>println(f))
    cosine_similarity_rdd
  }

  def trainData2(rdd:RDD[(String, String, Double)], frequent_pairs:RDD[(Array[String], Double)], sc:SparkContext):RDD[(String, String, Double)]={
    val Sr = new Srecommend()
    val rdd_perf = rdd.map(f=>(f._1, f._2, 70-f._3))
    val cosine_similarity_rdd = Sr.CosineSimilarity2(rdd_perf, frequent_pairs, sc)
    cosine_similarity_rdd.foreach(f=>println(f))
    cosine_similarity_rdd
  }

  def previewData(rdd:RDD[(String, String, Double)],cosine_rdd:RDD[(String, String, Double)]):RDD[(String, String, Double)]={
    val Sr = new Srecommend()
    val old_items = Sr.Get_old_items(rdd)
    val user_perf = rdd.map(f=>(f._1,f._2,65-f._3))
    val rec_rdd = Sr.Recommend(cosine_rdd,user_perf,old_items)
    rec_rdd.foreach(f => println(f))
    rec_rdd
  }

  def myFun(iterator: Iterator[(String, String, Double)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    //val sql = "insert into pre_fpitemcf_xg_rg_grade_2016(sno, cno, pre_score) values (?, ?, ?)"
    val sql = "insert into pre_itemcf_xg_rg_grade_2016(sno, cno, pre_score) values (?, ?, ?)"
    //val sql = "insert into pre_xg_rg_grade_2016(sno, cno, pre_score) values (?, ?, ?)"
    //val sql = "insert into pre_xg_rg_2016q(sno, cno, pre_score) values (?, ?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/data2","root", "")
      iterator.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, data._1)
        ps.setString(2, data._2)
        ps.setDouble(3, data._3)
        ps.executeUpdate()
      }
      )
    } catch {
      case e: Exception => println("Mysql Exception")
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def myFun_fp(iterator: Iterator[(String, String, String, Double)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into fpgrowth_result(antecedent1, antecedent2, consequent, confidence) values (?, ?, ?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/data2","root", "")
      iterator.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, data._1)
        ps.setString(2, data._2)
        ps.setString(3, data._3)
        ps.setDouble(4, data._4)
        ps.executeUpdate()
      }
      )
    } catch {
      case e: Exception => println("Mysql Exception")
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def myFun_fp_items(iterator: Iterator[(String, String, String, String)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into fpgrowth_result_items(item1, item2, item3, item4) values (?, ?, ?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/data2","root", "")
      iterator.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, data._1)
        ps.setString(2, data._2)
        ps.setString(3, data._3)
        ps.setString(4, data._4)
        ps.executeUpdate()
      }
      )
    } catch {
      case e: Exception => println("Mysql Exception")
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def myFun_kmeans(iterator: Iterator[(String, String)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into kmeans_result(cno,label) values (?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/data2","root", "")
      iterator.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, data._1)
        ps.setString(2, data._2)
        ps.executeUpdate()
      }
      )
    } catch {
      case e: Exception => println("Mysql Exception")
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def getFPGrowthData():RDD[(String, String)]={
    val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/data2")  //*****这是数据库名
      .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "student_rg")//*****是表名
      .option("user", "root").option("password", "").load()
    //fpgrowth_xg_data 已经去掉了2016级的成绩
    val temp_rdd = jdbcDF.rdd
    val result_rdd_ = temp_rdd.filter(_.getDouble(2)<70)
    val result_rdd_2 = result_rdd_.filter(!_.getString(4).equals("2016级"))
    val result_rdd = result_rdd_2.map(f=>(f.getString(0), f.getString(1)))
    result_rdd
  }

  def getKmeansTrainData():RDD[(Double,Double,Double,Double)]={
    val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/data2")  //*****这是数据库名
      .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "kmeans_data")//*****是表名
      .option("user", "root").option("password", "").load()

    val temp_rdd = jdbcDF.rdd
    val result_rdd = temp_rdd.map(f=>(f.getDouble(1), f.getDouble(2), f.getDouble(3), f.getDouble(4)))
    result_rdd
  }

  def getKmeansTestData():RDD[(String,(Double,Double,Double,Double))]={
    val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/data2")  //*****这是数据库名
      .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "kmeans_data")//*****是表名
      .option("user", "root").option("password", "").load()

    val temp_rdd = jdbcDF.rdd
    val result_rdd = temp_rdd.map(f=>(f.getString(0), (f.getDouble(1), f.getDouble(2), f.getDouble(3), f.getDouble(4))))
    result_rdd
  }

  def judgeFPArray(arr1:Array[String], arr2:Array[String], conf:Double):(String,String,String,String,String,Double)= {
    if (arr1.length == 1) {
      (arr1(0), null, null, null, arr2(0), conf)
    }
    else if(arr1.length == 2) {
      (arr1(0), arr1(1), null, null, arr2(0), conf)
    }
    else if(arr1.length == 3){
      (arr1(0), arr1(1), arr1(2), null, arr2(0), conf)
    }
    else{
      (arr1(0), arr1(1), arr1(2), arr1(3), arr2(0),  conf)
    }
//    else{
//      (arr1(0), arr1(1), arr1(2), arr2(0), conf)
//    }
  }

  //频繁项集判断
  def judgeFpArray2(arr:Array[String]):(String, String, String, String)={
    if(arr.length==1){
      (arr(0), null, null, null)
    }
    else if(arr.length==2){
      (arr(0), arr(1), null, null)
    }
    else if(arr.length==3){
      (arr(0), arr(1), arr(2), null)
    }
    else{
      (arr(0), arr(1), arr(2), arr(3))
    }
  }

  def fpgrowth(pairs:RDD[(String,String)]):RDD[(String,String,String,String,String,Double)]={
    //设置参数
    val minSupport=0.12//最小支持度
    val minConfidence=0.65//最小置信度
    val numPartitions=2//数据分区

    val grouped = pairs.groupByKey()
    println("-------------")
    grouped.foreach(every=>println(every))
    val values= grouped.map{group=>
      group._2.toList.mkString("\t")
    }
    values.foreach(x=>println(x))
    //val transactions=data.map(x=>x.split(" "))
    val transactions=values.map(x=>x.split("\t"))
    transactions.cache()
    //创建一个FPGrowth的算法实列
    val fpg = new FPGrowth()

    //设置训练时候的最小支持度和数据分区
    fpg.setMinSupport(minSupport)
    fpg.setNumPartitions(numPartitions)

    //把数据带入算法中
    val model = fpg.run(transactions)

    //查看所有的频繁项集，并且列出它出现的次数
    println("------------------------频繁项集：")
    model.freqItemsets.collect().foreach(itemset=>{
      println( itemset.items.mkString("[", ",", "]")+","+itemset.freq)
    })

    //通过置信度筛选出推荐规则则
    //antecedent表示前项
    //consequent表示后项
    //confidence表示规则的置信度
    //这里可以把规则写入到Mysql数据库中，以后使用来做推荐
    //如果规则过多就把规则写入redis，这里就可以直接从内存中读取了，我选择的方式是写入Mysql，然后再把推荐清单写入redis
    println("---------------------关联规则：")
    model.generateAssociationRules(minConfidence).collect().foreach(rule=>{
      println(
        rule.antecedent.mkString("[",",","]")+"-->"+
          rule.consequent.mkString("[",",","]")+","+rule.confidence
      )
    })

    val rdd_temp = model.generateAssociationRules(minConfidence).map(f=>judgeFPArray(f.antecedent,f.consequent,f.confidence))
    rdd_temp.foreach(f=>println(f))
    //查看规则生成的数量
    print(model.generateAssociationRules(minConfidence).collect().length)
    println("条规则")

//    try{
//      val file_out = new FileWriter("fp_growth_result.txt", false)
//      rdd_temp.foreach(line=>{
//        file_out.write(line._1)
//      })
//      file_out.close()
//    } catch {
//      case e:Exception => println(e)
//    }
    rdd_temp.coalesce(1).saveAsTextFile("./SparkTest/fp_growth_result")

    rdd_temp
    //并且所有的规则产生的推荐，后项只有1个，相同的前项产生不同的推荐结果是不同的行
    //不同的规则可能会产生同一个推荐结果，所以样本数据过规则的时候需要去重

  }

  def fpgrowth2(pairs:RDD[(String,String)]):RDD[(Array[String], Double)]={
    //设置参数
    val minSupport=0.15//最小支持度
    val minConfidence=0.65//最小置信度
    val numPartitions=2//数据分区

    val grouped = pairs.groupByKey()
    println("-------------")
    grouped.foreach(every=>println(every))
    val values= grouped.map{group=>
      group._2.toList.mkString("\t")
    }
    values.foreach(x=>println(x))
    //val transactions=data.map(x=>x.split(" "))
    val transactions=values.map(x=>x.split("\t"))
    transactions.cache()
    //创建一个FPGrowth的算法实列
    val fpg = new FPGrowth()

    //设置训练时候的最小支持度和数据分区
    fpg.setMinSupport(minSupport)
    fpg.setNumPartitions(numPartitions)

    //把数据带入算法中
    val model = fpg.run(transactions)

    //查看所有的频繁项集，并且列出它出现的次数
    println("------------------------频繁项集：")
    model.freqItemsets.collect().foreach(itemset=>{
      println( itemset.items.mkString("[", ",", "]")+","+itemset.freq)
    })
    println("频繁项集数量："+model.freqItemsets.collect().length)
    //通过置信度筛选出推荐规则则
    //antecedent表示前项
    //consequent表示后项
    //confidence表示规则的置信度
    //这里可以把规则写入到Mysql数据库中，以后使用来做推荐
    //如果规则过多就把规则写入redis，这里就可以直接从内存中读取了，我选择的方式是写入Mysql，然后再把推荐清单写入redis
    println("---------------------二项集：")
    //val rdd_temp = model.freqItemsets.filter(_.items.length==2)
    val rdd_temp = model.freqItemsets
    //val rdd_temp2 = rdd_temp.map(f=>(f.items(0),f.items(1),f.freq.toDouble))
    val rdd_temp2 = rdd_temp.map(f=>(f.items,f.freq.toDouble))
    rdd_temp2.foreach(f=>println(f))


    rdd_temp2
    //并且所有的规则产生的推荐，后项只有1个，相同的前项产生不同的推荐结果是不同的行
    //不同的规则可能会产生同一个推荐结果，所以样本数据过规则的时候需要去重

  }

  //返回频繁项集
  def fpgrowth3(pairs:RDD[(String,String)]):RDD[(String,String,String,String)]={
    //设置参数
    val minSupport=0.15//最小支持度
    val minConfidence=0.65//最小置信度
    val numPartitions=2//数据分区

    val grouped = pairs.groupByKey()
    println("-------------")
    grouped.foreach(every=>println(every))
    val values= grouped.map{group=>
      group._2.toList.mkString("\t")
    }
    values.foreach(x=>println(x))
    //val transactions=data.map(x=>x.split(" "))
    val transactions=values.map(x=>x.split("\t"))
    transactions.cache()
    //创建一个FPGrowth的算法实列
    val fpg = new FPGrowth()

    //设置训练时候的最小支持度和数据分区
    fpg.setMinSupport(minSupport)
    fpg.setNumPartitions(numPartitions)

    //把数据带入算法中
    val model = fpg.run(transactions)

    //查看所有的频繁项集，并且列出它出现的次数
    println("------------------------频繁项集：")
    model.freqItemsets.collect().foreach(itemset=>{
      println( itemset.items.mkString("[", ",", "]")+","+itemset.freq)
    })

    val rdd_items = model.freqItemsets.map(f=>judgeFpArray2(f.items))
    rdd_items
  }

  def savaResult(rdd: RDD[(String,String,Double)]):Unit={
    rdd.foreachPartition(myFun)
  }

  def saveResult_fp(rdd:RDD[(String,String,String,Double)]):Unit={
    rdd.foreachPartition(myFun_fp)
  }

  def saveResult_fp_items(rdd:RDD[(String, String, String, String)]):Unit={
    rdd.foreachPartition(myFun_fp_items)
  }

  def saveKmeans(rdd:RDD[(String, String)]):Unit={
    rdd.foreachPartition(myFun_kmeans)
  }
}

object Test2{
  def NowDate(): String={
    val now:Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sssss")
    val date = dateFormat.format(now)
    return date
  }
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf()
      .setAppName("map")
      .setMaster("local")
    val sc=new SparkContext(conf)
    val db = new SDBProcess()


    /*************************/

    //ItemCF+  fp增强相似矩阵
/*
    val rdd = db.getFPGrowthData()
    val fp_frequecy = db.fpgrowth2(rdd)
    val train_data2 = db.getTrainData()
    val train_result2 = db.trainData2(train_data2,fp_frequecy,sc)
    val preview_data2 = db.getPreviewData()
    val preview_result2 = db.previewData(preview_data2, train_result2)
    preview_result2.foreach(f=>println(f))
    db.savaResult(preview_result2)
*/
    /**********************/

    /**********************/
    //ItemCF original algorithm

/*
    val train_data = db.getTrainData()
    val s1 = NowDate()
    val now1 = new Date()
    val train_result = db.trainData(train_data)
    val now2: Date = new Date()
    val now3 = now2.getTime() - now1.getTime
    val dateFormat:SimpleDateFormat  = new SimpleDateFormat("mm:ss.sssss")
    val date = dateFormat.format(now3)
    println("train time:"+date)

    val preview_data = db.getPreviewData()

    val s2_1 = NowDate()
    val now2_1 = new Date()
    val preview_result = db.previewData(preview_data, train_result)
    val now2_2: Date = new Date()
    val now2_3 = now2_2.getTime() - now2_1.getTime
    val dateFormat2:SimpleDateFormat  = new SimpleDateFormat("mm:ss.sssss")
    val date2 = dateFormat2.format(now2_3)
    println("preview time:"+date2)

    val show = preview_result.sortBy(_._1)
    //show.foreach(f=>println(f))
   // db.savaResult(preview_result)
*/
    /***********************/
/*

    val train_data = db.getTrainData()
    //train_data.foreach(f=>println(f))
    val train_result = db.trainData(train_data)
    //train_result.foreach(f=>println(f))
    //rdd1.foreach(f=>println(f))
    val preview_data = db.getPreviewData()
    val preview_result = db.previewData(preview_data,train_result)
    preview_result.foreach(f=>println(f))
    db.savaResult(preview_result)
*/
    //rdd2.foreach(f=>println(f))
    /******************/

//val rdd = db.getFPGrowthData()
//    db.fpgrowth2(rdd)


    val rdd = db.getFPGrowthData()
    val fp_result = db.fpgrowth(rdd)



    /***
    //保存频繁项集
    val rdd = db.getFPGrowthData()
    val fp_result_items = db.fpgrowth3(rdd)
    db.saveResult_fp_items(fp_result_items)
    ***/

        //db.saveResult_fp(fp_result)

  }
}






