package com.atguigu.offline

import breeze.numerics.sqrt
import com.atguigu.offline.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: ECommerceRecommendSystem
  * Package: com.atguigu.offline
  * Version: 1.0
  *
  * Created by wushengran on 2019/4/27 11:24
  * 隐语义模型的预测评价
  * 方式一:通过模型训练的 RMAS ,RMAS越小,预测效果越好
  * 方式二:用户反馈:精确率和召回率
  */
object ALSTrainer {
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop03:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // 创建一个spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig( config("mongo.uri"), config("mongo.db") )

    // 加载数据
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(
      rating => Rating(rating.userId, rating.productId, rating.score)
    ).cache()

    // 数据集切分成训练集和测试集   看這個模型怎麽樣
    //RMSE最小，就是一個模型的最優
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD = splits(0)
    val testingRDD = splits(1)

    // 核心实现：输出最优参数
    adjustALSParams( trainingRDD, testingRDD )

    spark.stop()
  }

  def adjustALSParams(trainData: RDD[Rating], testData: RDD[Rating]): Unit ={
    // 定义模型训练的参数，rank隐特征个数，iterations迭代次数，lambda正则化系数
   //    val ( rank, iterations, lambda ) = ( 5, 10, 0.01 )
   //    val model = ALS.train( trainData, rank, iterations, lambda )
    // 遍历数组中定义的参数取值
    val result = for( rank <- Array(5, 10, 20, 50); lambda <- Array(1, 0.1, 0.01) )
      yield {
        val model = ALS.train(trainData, rank, 10, lambda)
        val rmse = getRMSE( model, testData )
        ( rank, lambda, rmse )
      }
    // 按照rmse排序并输出最优参数
    //rmse最小的参数
    println(result.minBy(_._3))

    //是随机的数据集划分,可能每次跑的结果不一样
    //(5,0.1,1.3172511462504775)
    //(5,0.1,1.325568278825698)
  }

  /*
  class MatrixFactorizationModel @Since("0.8.0") (
    @Since("0.8.0") val rank: Int,
    @Since("0.8.0") val userFeatures: RDD[(Int, Array[Double])],
    @Since("0.8.0") val productFeatures: RDD[(Int, Array[Double])])
   */
  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    // 构建userProducts，得到预测评分矩阵
    val userProducts = data.map( item=> (item.user, item.product) )
    //Predict the rating of many users for many products.
    //def predict(usersProducts: RDD[(Int, Int)]): RDD[Rating] = {  }
    val predictRating = model.predict(userProducts)

    // 按照公式计算rmse，首先把预测评分和实际评分表按照(userId, productId)做一个连接
    val observed = data.map( item=> ( (item.user, item.product),  item.rating ) )
    val predict = predictRating.map( item=> ( (item.user, item.product),  item.rating ) )

    //join就是一個内連接
    //sqrt 根號函數
    //mean 均方
    sqrt(
      observed.join(predict).map{
      case ( (userId, productId), (actual, pre) ) =>
        val err = actual - pre
        err * err
    }.mean()
    )
  }



}
