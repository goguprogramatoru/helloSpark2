import java.io.{File, FileWriter, BufferedWriter}
import java.text.SimpleDateFormat
import logic.Spark
import org.joda.time.{Days, DateTime}
import scala.reflect.io.Path
import scala.util.Try


object Application {

	val CSV_FOLDER = "/media/tempData/sparkDemo/csv/"
	val PQ_FOLDER = "/media/tempData/sparkDemo/pq/"
	val START_DAY = "2016-01-01"
	val END_DAY = "2016-01-03"

	val NB_ROWS_PER_DAY = 27*1000*1000

  	def main(args: Array[String]): Unit = {
		// Generate csv files:
		generateFiles()
		// convert csv to parquets
		generateParquets()
		// do sql query
		val SQL_QUERY = "SELECT product, shop, SUM(quantity),SUM(price) FROM data GROUP BY product, shop"
		doQuery(SQL_QUERY)

  	}

	def generateFiles() =  {
		val rowData = new StringBuilder()
		val r = scala.util.Random

		this.getDaysBetween(START_DAY,END_DAY).foreach(day =>
		{
			val startTime = System.currentTimeMillis()
			val bw = new BufferedWriter(new FileWriter(new File(CSV_FOLDER+day+".csv")))
			for (i <- 1 to NB_ROWS_PER_DAY) {
				rowData.clear()
				rowData
						.append(i).append(",")
						.append(r.nextInt(100)).append(",")
						.append(r.nextInt(100)).append(",")
						.append(r.nextInt(10000000)).append(",")
						.append(day).append(",")
						.append(r.nextInt(10)).append(",")
						.append((r.nextInt(999)+1)/10.0).append(",")
						.append(r.nextInt(1000)).append(",")
						.append(r.nextBoolean())
				bw.write(rowData.toString)
				if (i < NB_ROWS_PER_DAY) {
					bw.write("\n")
				}
			}
			bw.close()
			val endTime = System.currentTimeMillis()
			println("FINISHED "+day+" IN "+(endTime-startTime))
		}
		)

		println("Generate csv files : done.")
	}

	def generateParquets() = {
		this.getDaysBetween(START_DAY,END_DAY).foreach(day => {
			val startTime = System.currentTimeMillis()

			Try(Path(PQ_FOLDER+day+".pq").deleteRecursively())

			val df = Spark.csvToDf(CSV_FOLDER+day+".csv")
			Spark.dfToParquet(df.coalesce(1),PQ_FOLDER+day+".pq")
			val endTime = System.currentTimeMillis()
			println("FINISHED "+day+" IN "+(endTime-startTime))
		}
		)
		println("Generate parquets : done.")
	}

	def doQuery(sqlQuery:String) =  {

		val TABLE_NAME = "data"

		val startTime = System.currentTimeMillis()

		var result = ""
		try {

			val daysToRead = this.getDaysBetween(START_DAY,END_DAY).map(day => PQ_FOLDER+day+".pq").toArray[String]
			val df = Spark.multipleParquetToDf(daysToRead)
			//val df = Spark.parquetToDf(PQ_FOLDER+"2016-01-01.pq")
			Spark.registerTempTable(df,TABLE_NAME)

			val resultDf = Spark.sql(sqlQuery)

			result = resultDf.take(200).mkString("\n")
		}
		catch {
			case e: Exception => {
				println("Exception: " + e.getMessage)
				e.printStackTrace()
			}
		}

		val endTime = System.currentTimeMillis()

		println("took: " + (endTime - startTime) +"\n"+result)
	}

	private def getDaysBetween(startDate:String, endDate:String):Seq[String] = {
		val sdf = new SimpleDateFormat("yyyy-MM-dd")
		val fStartDate = new DateTime(sdf.parse(startDate))
		val fEndDate = new DateTime(sdf.parse(endDate))

		val numberOfDays = Days.daysBetween(fStartDate.toInstant, fEndDate.toInstant).getDays
		return (for (f <- 0 to numberOfDays) yield fStartDate.plusDays(f)).toList.map(e=> sdf.format(e.toDate))  //list string: yyyy-MM-dd
	}
}
