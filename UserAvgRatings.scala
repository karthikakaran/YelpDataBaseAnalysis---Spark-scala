import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object UserAvgRatings{
	def main(args: Array[String]){
		val conf = new SparkConf().setAppName("UserAvgRatings").setMaster("local");
  		val sc = new SparkContext(conf)
  		val userDet = sc.textFile("/home/vaishthiru/Downloads/user.txt")
		val reviewDet = sc.textFile("/home/vaishthiru/Downloads/review.txt")
		val userMap = userDet.filter(x => (x.split("\\^")(1) == "Matt A"))
		val userId = userMap.take(1)(0).split("\\^")(0).toInt
		val userMap = reviewDet.filter(x => (x.split("\\^")(1).toInt == userId))
		val idRatings = userMap.map(x => (x.split("\\^")(1), x.split("\\^")(3).toDouble))

		val countKey = idRatings.countByKey()	
		val parList = sc.parallelize(countKey.toList)
		val parMap = parList.map(t=>(t._1,t._2))
		val sumRate = idRatings.reduceByKey(_+_)
		val avgRatings = parMap.join(sumRate).map(t=>(t._1,t._2._2/t._2._1))
		avgRatings.foreach(y=>println("%s %s".format(y._1,y._2)))
	}
}
