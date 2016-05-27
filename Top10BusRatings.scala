import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Top10BusRatings{
	def main(args: Array[String]){
		val conf = new SparkConf().setAppName("Top10BusRatings").setMaster("local");
  		val sc = new SparkContext(conf)
  		val businessDet = sc.textFile(args(0))
		val reviewDet = sc.textFile(args(1))
		val busMap = businessDet.map(x => (x.split("\\^")(0), (x.split("\\^")(1)+","+x.split("\\^")(2))))
		val reviewMap = reviewDet.map(x => (x.split("\\^")(2), x.split("\\^")(3)).toInt)
		val countKey = reviewMap.countByKey()	
		val parList = sc.parallelize(countKey.toList)
		val parMap = parList.map(t=>(t._1,t._2))
		val sumRate = reviewMap.reduceByKey(_+_)
		val avgRatings = parMap.join(sumRate).map(t=>(t._1,t._2._2/t._2._1))
		avgRatings.join(busMap).foreach(y=>println("%s %s %s".format(y._1,y._2._2,y._2._1)))
	}
}
