package edu.luc.cs.apache.spark.examples
/**
  * Created by Shilpika on 1/27/16.
  */

/**
  * Computes the PageRank of URLs from an input file. Input file should
  * be in format of:
  * URL         neighbor URL
  * URL         neighbor URL
  * URL         neighbor URL
  * ...
  * where URL and their neighbors are separated by space(s).
  *
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * please refer to org.apache.spark.graphx.lib.PageRank
  */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SparkPageRank {

  def displayWarning(): Unit ={
    System.err.println(
      """
        |WARN: This is just a naive implementation of PageRank and is given as an example
        |TODO
      """.stripMargin)
  }

  def mainForPage(args: Array[String]): Unit = {

    if(args.length <1){
      System.err.println("Usage: SparkPageRank <Filename> <iter_count>")
      System.exit(1)
    }
    displayWarning()

    val conf = new SparkConf().setAppName("Page Rank")
    val sc = new SparkContext(conf)
    //println(args(0)+" !!!!")
    val iter = if(args(1).length >1) args(1).toInt else 10
    val input = sc.textFile(args(0),1)
    //println(input.collect().mkString)
    val links = input.map(x => {
      val urls = x.split("\\s+")
      (urls(0),urls(1))
    }).distinct().groupByKey().cache()
    var ranks = links.mapValues(_=>1.0)

    for (i <- 1 to iter) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val output = ranks.collect()
    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

    sc.stop()
  }

}
