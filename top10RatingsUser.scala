import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object top10RatingsUser{
	def main(args: Array[String]){
		val conf = new SparkConf().setAppName("top10RatingsUser").setMaster("local");
  		val sc = new SparkContext(conf)
  		val reviewDet = sc.textFile("/home/vaishthiru/Downloads/review.txt")
		val userDet = sc.textFile("/home/vaishthiru/Downloads/user.txt")
		val userStar = reviewDet.map(t=>(t.split("\\^")(1),t.split("\\^")(3)))
		val userIdName = userDet.map(t=>(t.split("\\^")(0),t.split("\\^")(1)))
		val topTenUsers = sc.parallelize(userStar.countByKey().toList.sortBy(_._2).reverse.take(3))
		userIdName.join(topTenUsers).foreach(y=>println("%s %s".format(y._1,y._2._1)))
	}
}
