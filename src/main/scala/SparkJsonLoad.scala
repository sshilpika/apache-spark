package edu.luc.cs.apache.spark.examples

/**
  * Created by Shilpika on 1/29/16.
  * This is a simple example of Spark to used to parse Json data in Scala
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature


case class Person(name: String, lovesJson: Boolean)
object SparkJsonLoad {

  def main(args:Array[String]): Unit ={
    if(args.length<3){
      System.err.print("Usage: [sparkmaster] [inputfile] [outputfile]")
      System.exit(1)
    }
    val master = args(0)
    val inputFile = args(1)
    val outputFile = args(2)

    val conf = new SparkConf().setMaster(master).setAppName("SparkJsonParserScalaJackson").setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)

    val input = sc.textFile(inputFile)

    val result = input.mapPartitions(records =>{
      //set mapper for each execution node
      val mapper = new ObjectMapper //with ScalaObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false)
      mapper.registerModule(DefaultScalaModule)

      records.flatMap(record =>{
        try{
          Some(mapper.readValue(record,classOf[Person]))
        }catch {
          case e: Exception => None
        }
      })
    },true)

    result.filter(!_.lovesJson).mapPartitions(record =>{
      val mapper = new ObjectMapper //with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      record.map(mapper.writeValueAsString(_))
    }).saveAsTextFile(outputFile)
  }

}
