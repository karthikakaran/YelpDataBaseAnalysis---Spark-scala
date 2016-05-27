import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object userRatingatStanford{
	def main(args: Array[String]){
		val conf = new SparkConf().setAppName("userRatingatStanford").setMaster("local");
  		val sc = new SparkContext(conf)
  		val reviewDet = sc.textFile("/home/vaishthiru/Downloads/review.txt")
		val busDet = sc.textFile("/home/vaishthiru/Downloads/business.txt")
		val busIds = busDet.filter(x => (x.split("\\^")(1).contains("Stanford"))).map(t=>(t.split("\\^")(0),1))
		val userStar = reviewDet.map(t=>(t.split("\\^")(2),t.split("\\^")(3)))
		userStar.join(busIds).foreach(y=>println("%s %s".format(y._1,y._2._1)))
	}
}
