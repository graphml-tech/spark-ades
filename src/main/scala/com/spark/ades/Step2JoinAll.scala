/**
 * All rights reserved.
 * @author Qiuzhuang.Lian
 */
package com.spark.ades

import com.spark.ades.Model._
import java.lang._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

object Step2JoinAll {

  def demoCounts(sc: SparkContext): RDD[((String, Long, String), Int)] = {
    val demoCountsRdd = sc.textFile(s"${dataUrlPrefix}aers/strat_demo_counts")
    demoCountsRdd.map{ line =>
      val splits = line.split("\\$")
      ((splits(0), Long.parseLong(splits(1)), splits(2)), Integer.parseInt(splits(3)))
    }
  }

  def drugCounts(sc: SparkContext): RDD[((String, Long, String, String), Int)] = {
    val drugCountsRdd = sc.textFile(s"${dataUrlPrefix}aers/strat_drugs_counts")
    drugCountsRdd.map { line =>
      val splits = line.split("\\$")
      ((splits(0), Long.parseLong(splits(1)), splits(2), splits(3)), Integer.parseInt(splits(4)))
    }
  }

  def reacsCounts(sc: SparkContext): RDD[((String, Long, String, String), Int)] = {
    val reacsCountsRdd = sc.textFile(s"${dataUrlPrefix}aers/strat_reacs_counts")
    reacsCountsRdd.map { line =>
      val splits = line.split("\\$")
      ((splits(0), Long.parseLong(splits(1)), splits(2), splits(3)), Integer.parseInt(splits(4)))
    }
  }

  def d2rCounts(sc: SparkContext): RDD[(String, Long, String, String, String, String, Int)] = {
    val d2rCountsRdd = sc.textFile(s"${dataUrlPrefix}aers/strat_drugs2_reacs_counts")
    d2rCountsRdd.map { line =>
      val splits = line.split("\\$")
      (splits(0), Long.parseLong(splits(1)), splits(2), splits(3), splits(4),
          splits(5), Integer.parseInt(splits(6)))
    }
  }

  def genDrug2DrugReacActualAndExpected(sc: SparkContext): Unit = {
    val d2rCountsRdd = d2rCounts(sc)
    val d2gDemoCountsPair = d2rCountsRdd.map( {
      case (gender, age, fdaDt, drug1, drug2, react, count) =>
        ((gender, age, fdaDt), (drug1, drug2, react, count))
    })
    val joinDemoCountsPair = d2gDemoCountsPair.join(demoCounts(sc)).map( {
      case ((gender, age, fdaDt), (v1, v2)) => ((gender, age, fdaDt, v1._1), (v1._2, v1._3, v1._4, v2))
    })

    val drugCountsRdd = drugCounts(sc)
    val joinD1CountsRdd = joinDemoCountsPair.join(drugCountsRdd).map( {
      case ((gender, age, fdaDt, drug1), (v1, drug1Count)) =>
        ((gender, age, fdaDt, v1._1), (drug1, v1._2, v1._3, v1._4, drug1Count))
    })

    val joinD2CountsRdd = joinD1CountsRdd.join(drugCountsRdd).map( {
      case ((gender, age, fdaDt, drug2), (v1, drug2Count)) =>
        ((gender, age, fdaDt, v1._2), (v1._1, drug2, v1._3, v1._4, v1._5, drug2Count))
    })

    val joinReacsCountsRdd = joinD2CountsRdd.join(reacsCounts(sc)).map( {
      case ((gender, age, fdaDt, react), (v1, reactCount)) =>
        (gender, age, fdaDt, v1._1, v1._2, react, v1._3, v1._4, v1._5, v1._6, reactCount)
    })

    val actualExpectedRdd = joinReacsCountsRdd.map( {
      case (gender, age, fdaDt, drug1, drug2, react, count, demoCount, drug1Count, drug2Count, reactCount) =>
        (gender, age, fdaDt, drug1, drug2, react, count,
            (drug1Count*drug2Count*reactCount)/(1.0*demoCount*demoCount))
    })

    actualExpectedRdd.groupBy(x => (x._4, x._5, x._6)).filter(x => !x._2.isEmpty)
      .map( { case (k, list) =>
        var actual  = 0L
        var exptected = 0.0D
        for (item <- list) {
          actual = actual + item._7
          exptected = exptected + item._8
        }
        (k._1 + "$" + k._2 + "$" + k._3 + "$" + actual + "$" + exptected)
    }).saveAsTextFile(s"${dataUrlPrefix}aers/drugs2_reacs_actual_expected")
  }

  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("Step2JoinAll")
    val sc = new SparkContext(sparkConf)
    genDrug2DrugReacActualAndExpected(sc)
    sc.stop()
    println("Done with ADES Step2JoinAll!")
  }
}
