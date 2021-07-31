import breeze.numerics.sqrt
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class Srecommend extends Serializable {
  def Get_old_items(rdd:RDD[(String,String,Double)]):(RDD[String])={
    val rdd2 = rdd.map(line => line._2)
    val rdd3 = rdd2.distinct()
    rdd3
  }
  //获取用户评分RDD
  def UserData(sc:SparkContext, path:String, str_split:String):(RDD[(String,String,Double)])={
    val user_rdd = sc.textFile(path)
    val user_rdd1 = user_rdd.map(line => {
      val lines = line.split(str_split)
      (lines(0),lines(1),lines(2).toDouble)
    })
    user_rdd1
  }

  def CosineSimilarity (
                         user_rdd:RDD[(String,String,Double)]
                       ) : (RDD[(String,String,Double)]) = {
    //  0 数据做准备
    val user_rdd2=user_rdd.map(f => (f._1,(f._2,f._3))).sortByKey()
    user_rdd2.cache
    //  1 (用户,物品,评分)笛卡尔积 (用户,物品,评分) =>（物品1,物品2,评分1,评分2）组合
    val user_rdd3=user_rdd2.join(user_rdd2)
    val user_rdd4=user_rdd3.map(f=> ((f._2._1._1, f._2._2._1),(f._2._1._2, f._2._2._2)))
    //  2 （物品1,物品2,评分1,评分2）组合 => （物品1,物品2,评分1*评分2）组合并累加
    val user_rdd5=user_rdd4.map(f=> (f._1,f._2._1*f._2._2 )).reduceByKey(_+_)
    //  3 对角矩阵
    val user_rdd6=user_rdd5.filter(f=> f._1._1 == f._1._2)
    //  4 非对角矩阵
    val user_rdd7=user_rdd5.filter(f=> f._1._1 != f._1._2)
    //  5 计算相似度
    val user_rdd6s = user_rdd6.map(f=>(f._1._1,f._2))
    val user_rdd8=user_rdd7.map(f=> (f._1._1, (f._1._1, f._1._2, f._2))).join(user_rdd6s)
    val user_rdd9=user_rdd8.map(f=> (f._2._1._2, (f._2._1._1,
      f._2._1._2, f._2._1._3, f._2._2)))
    val user_rdd10=user_rdd9.join(user_rdd6s)
    val user_rdd11 = user_rdd10.map(f => (f._2._1._1,f._2._1._2,f._2._1._3,f._2._1._4,f._2._2))
    val user_rdd12=user_rdd11.map(f=> (f._1, f._2, (f._3 / sqrt(f._4 * f._5)) ))
    //  7 结果返回
    user_rdd12
  }

  def CosineSimilarity2 (
                         user_rdd:RDD[(String,String,Double)],
                         frequent_pairs:RDD[(Array[String], Double)],
                         sc:SparkContext
                       ) : RDD[(String,String,Double)] = {
    //  0 数据做准备
    val user_rdd2=user_rdd.map(f => (f._1,(f._2,f._3))).sortByKey()
    user_rdd2.cache
    //  1 (用户,物品,评分)笛卡尔积 (用户,物品,评分) =>（物品1,物品2,评分1,评分2）组合
    val user_rdd3=user_rdd2.join(user_rdd2)
    val user_rdd4=user_rdd3.map(f=> ((f._2._1._1, f._2._2._1),(f._2._1._2, f._2._2._2)))
    //  2 （物品1,物品2,评分1,评分2）组合 => （物品1,物品2,评分1*评分2）组合并累加
    val user_rdd5=user_rdd4.map(f=> (f._1,f._2._1*f._2._2 )).reduceByKey(_+_)
    //  3 对角矩阵
    val user_rdd6=user_rdd5.filter(f=> f._1._1 == f._1._2)
    //  4 非对角矩阵
    val user_rdd7=user_rdd5.filter(f=> f._1._1 != f._1._2)
    //  5 计算相似度
    val user_rdd6s = user_rdd6.map(f=>(f._1._1,f._2))
    val user_rdd8=user_rdd7.map(f=> (f._1._1, (f._1._1, f._1._2, f._2))).join(user_rdd6s)
    val user_rdd9=user_rdd8.map(f=> (f._2._1._2, (f._2._1._1,
      f._2._1._2, f._2._1._3, f._2._2)))
    val user_rdd10=user_rdd9.join(user_rdd6s)
    val user_rdd11 = user_rdd10.map(f => (f._2._1._1,f._2._1._2,f._2._1._3,f._2._1._4,f._2._2))
    val user_rdd12=user_rdd11.map(f=> (f._1, f._2, (f._3 / sqrt(f._4 * f._5)) ))
    //  7 结果返回
   // println("初始相似矩阵")
    //user_rdd12.foreach(f=>println(f))
    //对频繁项集加权增强

/*
    var course1:Array[String] = new Array[String](frequent_pairs.collect().length+5)
    var course2:Array[String] = new Array[String](frequent_pairs.collect().length+5)
    var freq:Array[Double] = new Array[Double](frequent_pairs.collect().length+5)
    freq.foreach(f=>0)
    var id = 0
    user_rdd12.collect().foreach(f=>{
      course1(id) = f._1
      course2(id) = f._2
      id = id + 1
    })
    id = 0
*/
//    val user_rdd13 = user_rdd12.map(f=>intense(f,frequent_pairs.collect()))
    var array1 = user_rdd12.collect()
    var array2 = frequent_pairs.collect()
    var sum:Double = 0
    array2.foreach(f=>{
      sum = sum + f._2
    })

    var i = 0
    while(i < array1.length){
      var cnt:Double = 0
      for(j <- array2){
        if(j._1.contains(array1(i)._1)&&j._1.contains(array1(i)._2)){
          cnt = cnt + j._2
        }
        /**********/
        /*
        if( (array1(i)._1.equals(j._1)&&array1(i)._2.equals(j._2))||(array1(i)._2.equals(j._1)&&array1(i)._1.equals(j._2)) ){
          var temp = array1(i)._3 + j._3
          array1(i) = (array1(i)._1, array1(i)._2, temp)
        }
        */
        /**********/
      }
      var exp:Double = cnt/sum
      if(cnt!=0) {
        print(exp + " " + cnt + " " + sum + " " + array1(i)._3 + " ")

      }
      array1(i) = (array1(i)._1, array1(i)._2, array1(i)._3 + exp*32)
      if(cnt!=0)
        {
          println(array1(i)._3)
        }
      i = i+1
    }
    val user_rdd13 = sc.parallelize(array1)

    //println("增强相似矩阵"

    user_rdd13
  }

  def intense(line:(String, String, Double), frequent_pairs:Array[(String, String, Double)]):(String, String, Double)={
    var temp = line
    for(i <- frequent_pairs){
      if((line._1.equals(i._1)&&line._2.equals(i._2))||(line._1.equals(i._2)&&line._2.equals(i._1))){
        temp = (line._1, line._2, line._3+i._3)
      }
    }
    temp
  }

  def Cooccurrence (
                     user_rdd:RDD[(String,String,Double)]
                   ) : (RDD[(String,String,Double)]) = {
    //  0 数据做准备 rdd2: (A,a) (A,b) (B,a) (B,c) (C,a)  (C,b)  (C,c)
    val user_rdd2=user_rdd.map(f => (f._1,f._2)).sortByKey()
    user_rdd2.cache()
    //  1 (用户：物品)笛卡尔积 (用户：物品 =>物品:物品组合
    //  rdd3: (A,(a,b)) (A,(b,a)) (B,(a,c)) (B,(c,a)) (C,(a,b)) (C,(a,c)) (C,(b,a)) (C,(b,c)) (C,(c,a)) (C,(c,b))
    val user_rdd3 = user_rdd2.join(user_rdd2)
    //  rdd4: ((a,b),1) ((b,a),1) ((a,c),1) ((c,a),1) ((a,b),1) ((a,c),1) ((b,a),1) ((b,c),1) ((c,a),1) ((c,b),1)
    val user_rdd4 = user_rdd3.map(data=> (data._2,1))
    //  2 物品:物品:频次
    //  rdd5: ((a,b),2) ((b,a),2) ((a,c),2) ((c,a),2) ((b,c),1) ((c,b),1)
    val user_rdd5 = user_rdd4.reduceByKey((x,y) => x + y)
    //  3 对角矩阵
    //  rdd6: ((a,a),3) ((b,b),2) ((c,c),2)
    val user_rdd6 = user_rdd5.filter(f=> f._1._1 == f._1._2)
    //  4 非对角矩阵
    //  rdd7: ((a,b),2) ((b,a),2) ((a,c),2) ((c,a),2) ((b,c),1) ((c,b),1)
    val user_rdd7 = user_rdd5.filter(f=> f._1._1 != f._1._2)
    //  5 计算同现相似度（物品1，物品2，同现频次）
    //  rdd8: (a,((a,b,2),3)) (a,((a,c,2),3)) (b,((b,a,2),2)) (b,((b,c,1),2)) (c,((c,a,2),2)) (c,(c,b,1),2)
    val user_rdd6s = user_rdd6.map(f => (f._1._1,f._2))
    val user_rdd8 = user_rdd7.map(f=> (f._1._1, (f._1._1,
      f._1._2, f._2))).join(user_rdd6s)  //user_rdd6s 换成 user_rdd6.map(f=>(f._1._1,f,_2))会报错
    val user_rdd9 = user_rdd8.map(f=> (f._2._1._2, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._2)))
    val user_rdd10 = user_rdd9.join(user_rdd6s)
    val user_rdd11 = user_rdd10.map(f => (f._2._1._1,f._2._1._2,f._2._1._3,f._2._1._4,f._2._2))
    val user_rdd12 = user_rdd11.map(f=> (f._1, f._2, (f._3 / sqrt(f._4 * f._5)) ))
    //   6结果返回
    user_rdd12
  }

  def Filter(line:((String,String),Double),items:Array[String]):Boolean={
    var flag = true

    for(i <- 0 to 99){
      if(items(i)==line._1._2){
        flag = false
      }
    }
    flag
  }
  def Recommend (
                  items_similar:RDD[(String,String,Double)],
                  user_perf:RDD[(String,String,Double)],
                  old_items:RDD[String]
                ) : (RDD[(String,String,Double)]) = {
    //  1 矩阵计算——i行与j列join
    val user_perfs = user_perf.map(f => (f._2, (f._1,f._3)))
    val rdd_app1_R2 = items_similar.map(f => (f._2, (f._1,f._3))).
      join(user_perfs) //rdd_app1_R2: (item2, ((user, item2_score), (item1, sim_1_2)))
    val rdd_app1_R2_0 = rdd_app1_R2.map(f=>(f._2._2._1,((f._2._1._1, f._2._1._2), (f._1, f._2._2._2))))
    //rdd_app1_R2_0:(item1, ((user, item2_score), (item2, sim_1_2)))
    /*****
      * 保留topN近邻居
      */
    val rdd_app1_R2_1 = rdd_app1_R2_0.groupByKey()
    val rdd_app1_R2_2 = rdd_app1_R2_1.map(f=>{
      val i = f._2.toBuffer
      val i2 = i.sortBy(_._2._2)
      val i3 = i2.reverse
      val kk = 50
      if(i3.length>kk){
        i3.remove(kk,i3.length-kk)
      }
      (f._1,i3.toIterable)
    })
    val rdd_app1_R2_3 = rdd_app1_R2_2.flatMap(f=>{
      val id2 = f._2
      val len = id2.toBuffer.length
      //逐一取出每个sno对应的item，并存入数组中
      for (w <-id2)yield(f._1, w, len)
    })
    val rdd_app1_R2_4 = rdd_app1_R2_3.map(f=>(f._2._2._1, ((f._2._1._1, f._2._1._2), (f._1, f._2._2._2)), f._3))
    //  2 矩阵计算——i行与j列元素相乘
    val rdd_app1_R3 = rdd_app1_R2_4.map(f=> ((f._2._2._1,f._2._1._1, f._3),f._2._2._2*f._2._1._2)) //rdd_app1_R3: ((item1, user, len), sim_1_2 * item2_score)
    //val rdd_app1_R3_map = rdd_app1_R3.map(f=>((f._1._1,f._1._2), Math.atan(f._2)))
    // rdd_app1_R3_map.foreach(f=>println("@"+f))
    //  3 矩阵计算——用户：元素累加求和
    //val strs:Array[String] = new Array[String](100)

    // 不需要排除old items, 需对结果计算精确度和召回率
    /*
    var id = 0
    old_items.collect.foreach(f=>{
      strs(id) = f
      id = id+1
    })
    */
    //strs.foreach(f=>println(f))
    //println("test**************")
    //val test = rdd_app1_R3.sortByKey()
    //test.foreach(f=>println(f))
   // val rdd_app1_R3_filter = rdd_app1_R3.filter(Filter(_,strs))  无需过滤old items
    //val rdd_app1_R4 = rdd_app1_R3_filter.reduceByKey((x,y)=> x+y).map(f => (f._1._1,(f._1._2,f._2)))
    val rdd_app1_R4 = rdd_app1_R3.reduceByKey((x,y)=> x+y)
    //  4 矩阵计算——用户：用户对结果排序，过滤
    //val k2 = 10.0
    val rdd_app1_R4_map = rdd_app1_R4.map(f => (f._1._1,(f._1._2, f._2/f._1._3.toDouble)))
    //val rdd_app1_R4_map = rdd_app1_R4_0.map(f=>(f._1, (f._2._1, Math.atan(f._2._2))))
    val rdd_app1_R5 = rdd_app1_R4_map.groupByKey()
    val rdd_app1_R6 = rdd_app1_R5.map(f=> {
      val i2 = f._2.toBuffer
      val i2_2 = i2.sortBy(_._2)
      var iid = 0
      for(i <- i2_2){
        //1.48999
        //1.53000
        //1.54999
        if(i._2 > 0.98){
          iid = iid +1
        }
      }
      //移除预测分小于阈值的item
      if (i2_2.length > iid)i2_2.remove(0,(i2_2.length-iid))
      (f._1,i2_2.toIterable)
    })
    val rdd_app1_R7 = rdd_app1_R6.flatMap(f=> {
      val
      id2 = f._2
      //逐一取出每个sno对应的item，并存入数组中
      for (w <-id2)yield(f._1,w._1,w._2)
    })
    rdd_app1_R7
  }




}
