/* WikiPageViewStats.scala */
package mids.w251.wiki

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config

object WikiPageViewStats {

    // Define the schema using a case class.
    case class rawWikiStats(project: String, page: String, pageViews: Long, bytes: Long)

    def main(args: Array[String]) {
        // load app config
        val conf:Config = ConfigFactory.load()

        // main entry point for spark Streaming functionality
        val sparkConf = new SparkConf().setAppName("WikiPageViewStats").setMaster("local[4]")
        val sc = new SparkContext(sparkConf)
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._

        val stats = sc.textFile("/root/w251/test/pagecounts-20090301-010000").map(_.split(" ")).map(p => rawWikiStats(p(0), p(1), p(2).trim.toLong, p(3).trim.toLong)).toDF()
        stats.registerTempTable("stats")

        // SQL statements can be run by using the sql methods provided by sqlContext.
        val pageviews = sqlContext.sql("SELECT page FROM stats GROUP BY project, page HAVING COUNT(*) > 1").collect().foreach(println)
        // pageviews.rdd.saveAsTextFile("query1.csv")
    }
}