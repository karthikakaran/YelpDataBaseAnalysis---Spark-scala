import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object busRatingsCount{
	def main(args: Array[String]){
		val conf = new SparkConf().setAppName("busRatingsCount").setMaster("local");
  		val sc = new SparkContext(conf)
  		val reviewDet = sc.textFile("/home/vaishthiru/Downloads/review.txt")
		val busDet = sc.textFile("/home/vaishthiru/Downloads/business.txt")
		val txBusIds = busDet.filter(t=>(t.split("\\^")(1).contains("TX"))).map(x=>(x.split("\\^")(0),0))
		val userStar = reviewDet.map(t=>(t.split("\\^")(2),t.split("\\^")(3)))
		txBusIds.join(userStar).map(t=>(t._1,t._2._2)).countByKey().foreach(println)
	}
}
