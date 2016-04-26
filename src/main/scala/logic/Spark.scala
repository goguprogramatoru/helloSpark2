package logic

/**
 * Created by mourad.benabdelkerim on 4/26/16.
 */

import java.sql.Date
import java.text.SimpleDateFormat

import models.{Sale}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

object Spark {

	//val SPARK_SERVER_ADDRESS = "spark://Spark1:7023"
	val SPARK_SERVER_ADDRESS = "local[8]"
	val APPLICATION_NAME ="Spark demo"
	val JAR_PATH ="/media/application.jar"

	val conf = new SparkConf()
			.setMaster(SPARK_SERVER_ADDRESS)
			.setAppName(APPLICATION_NAME)

	val sc = new SparkContext(conf)
	//sc.addJar(JAR_PATH)
	val sqlContext = new SQLContext(sc)


	def disconnect() = {
		sc.stop()
	}

	def csvToDf(filePath:String):DataFrame = {
		import sqlContext.implicits._

		val sdf = new SimpleDateFormat("yyyy-MM-dd")


		val rdd = sc
				.textFile(filePath)
				.map(_.replaceAll("\"", "").split(","))
				.map(e =>
					Sale(
						e(0).toLong,
						e(1).toInt,
						e(2).toInt,
						e(3).toInt,
						new java.sql.Date(sdf.parse(e(4)).getTime),
						e(5).toInt,
						e(6).toFloat,
						e(7).toInt,
						e(8).toBoolean
					)
				)
		return rdd.toDF()
	}

	def registerTempTable(df:DataFrame, tableName:String) = {
		df.registerTempTable(tableName)
	}

	def sql(sql: String): DataFrame = {
		return sqlContext.sql(sql)
	}

	def dfToParquet(df: DataFrame, savePath: String) = {
		df.write.save(savePath)
	}

	def parquetToDf(path: String): DataFrame = {
		return sqlContext.read.load(path)
	}

	def multipleParquetToDf(paths:Array[String]): DataFrame ={
		return sqlContext.read.load(paths:_*)
	}
}
