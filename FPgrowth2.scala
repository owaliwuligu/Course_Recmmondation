package spark.core

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.mllib.fpm.FPGrowthModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object FPgrowth2{
  //关联规则挖掘是在满足一定支持度的情况下寻找置信度达到阈值的所有模式，设置最小的置信度筛选可靠的规则
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("FPGrowthTest").setMaster("local")
    val sc = new SparkContext(conf)

    //设置参数
    val minSupport=0.2//最小支持度
    val minConfidence=0.2//最小置信度
    val numPartitions=2//数据分.区

    //读取数据
    //al data = sc.textFile("D:/BigData/fpSampleData2.txt")
    val data = sc.textFile("E:/Spark/failedScore3.txt")

    //把数据通过tab分割
    val pairs = data.map{x =>
      (x.split("\t")(0),x.split("\t")(1))
    }
    val grouped = pairs.groupByKey()
    val values= grouped.map{group=>
      group._2.toList.mkString("\t")
    }
    values.foreach(x=>println(x))
    //al transactions=data.map(x=>x.split(" "))
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
    println("频繁项集：")
    model.freqItemsets.collect().foreach(itemset=>{
      println( itemset.items.mkString("[", ",", "]")+","+itemset.freq)
    })

    //通过置信度筛选出推荐规则则
    //antecedent表示前项
    //consequent表示后项
    //confidence表示规则的置信度
    //这里可以把规则写入到Mysql数据库中，以后使用来做推荐
    //如果规则过多就把规则写入redis，这里就可以直接从内存中读取了，我选择的方式是写入Mysql，然后再把推荐清单写入redis
    println("规则：")
    model.generateAssociationRules(minConfidence).collect().foreach(rule=>{
      println(
        rule.antecedent.mkString(",")+"-->"+
          rule.consequent.mkString(",")+"-->"+ rule.confidence
      )
    })
    //查看规则生成的数量
    println(model.generateAssociationRules(minConfidence).collect().length)


    //并且所有的规则产生的推荐，后项只有1个，相同的前项产生不同的推荐结果是不同的行
    //不同的规则可能会产生同一个推荐结果，所以样本数据过规则的时候需要去重

  }
}
