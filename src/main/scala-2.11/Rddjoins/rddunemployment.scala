package Rddjoins

import org.apache.spark.{SparkConf, SparkContext}




import scalafx.application.JFXApp

case class Area(code:String,country:String)
case class dat(id:String,year:Int,period:Int,value:Double)
case class series(id:String,areaC:String,measure:Int,title:String)
/**
  * Created by maheshtikky on 12/15/17.
  */
object rddunemployment extends App {

  val conf = new SparkConf().setAppName("Unemploymentdata").setMaster("local[*]").set("spark.driver.host", "localhost")
  val sc = new SparkContext(conf)

  sc.setLogLevel("WARN")

  val area = sc.textFile("/Users/maheshtikky/Documents/ppk/target/la.area")
    .filter(!_.contains("area_type"))
    .map({ line =>
      val p = line.split("\t")
        .map(_.trim)
      Area(p(1), p(2))
    }).cache()

  area.take(5) foreach println

  val serie = sc.textFile("/Users/maheshtikky/Documents/ppk/target/la.series.txt")
    .filter(!_.contains("area_code"))
    .map({ line =>
      val p = line.split("\t")
        .map(_.trim)
      series(p(0), p(2), p(3).toInt, p(6))
    }).cache()
  serie.take(5) foreach println

  val data = sc.textFile("/Users/maheshtikky/Documents/ppk/target/la.data.30.Minnesota.txt")
    .filter(!_.contains("year"))
    .map({ line =>
      val p = line.split("\t")
        .map(_.trim)
      dat(p(0), p(1).toInt, p(2).drop(1).toInt, p(3).toDouble)
    }).cache()
  data.take(5) foreach println

  val rate = data.filter(_.id.endsWith("03"))
  val decadegroup = rate.map(l => (l.id, l.year / 10) -> l.value)
  val aggre = decadegroup.aggregateByKey(0.0 -> 0)({ case ((s, c), d) => (s + d, c + 1) }, { case ((s1, c1), (s2, c2)) => (s1 + s2, c1 + c2) })
    .mapValues(t => t._1 / t._2)
  val maxdecade = aggre.map { case ((id, dec), ave) => id -> (dec, ave) }.reduceByKey { case ((d1, a1), (d2, a2)) => if (a1 >= a2) (d1, a1) else (d2, a2) }
  maxdecade.take(5) foreach println

  val seriespair = serie.map(t => t.id -> t.title)
  val joined2data = seriespair.join(maxdecade)
  joined2data.take(5) foreach println

  val aggregagg= joined2data.mapValues({case (a,(b,c)) => (a,b,c)}).map( {case (id,t) => id.drop(3).dropRight(2) -> t})
  aggregagg.take(5) foreach println

  val joindataarea = area.map(i => i.code -> i.country).join(aggregagg)
  joindataarea.take(10) foreach println

}
