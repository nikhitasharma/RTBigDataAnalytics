import org.apache.spark.{SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

object SparkTransformation {
  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir","C:\\Users\\Nikhita_Sharma\\Downloads\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkTransformation").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val inputText = sc.textFile("data/elections.txt")
    val hillary_count = inputText.filter(line => line.contains("Clinton")).count()
    val trump_count = inputText.filter(line => line.contains("Trump")).count()
    val hillary_lines = inputText.filter(line => line.contains("Clinton"))
    //hillary_lines.saveAsTextFile("data/hillary_lines")
    val trump_lines = inputText.filter(line => line.contains("Trump"))
    val trump_lines_Array = trump_lines.collect()
    //val TLinesRDD = sc.parallelize(trump_lines)
    println( "Hillary Count:" + hillary_count)
    println("Lines with Hillary clinton:")
    hillary_lines.foreach(println)

    println( "Trump Count:" + trump_count)
    println("Lines with Donald Trump:")
    trump_lines.foreach(println)



  }

}
