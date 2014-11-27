/**
 * All rights reserved.
 * @author Qiuzhuang.Lian
 */
package com.spark.ades

import com.spark.ades.Model._

import java.lang._
import java.util.Collections

import com.cloudera.science.quantile.MunroPatersonQuantileEstimator
import org.apache.spark._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object Step3DrugReactStat {

  private[this] def bin(expected: Double, sortedQuantiles: Seq[Double]): Int = {
    val index = Collections.binarySearch(sortedQuantiles, expected)
    if (index > -1) {
      index
    } else {
      -index-1
    }
  }

  def drugs2ReacsStat(sc: SparkContext, actualThreshold: Int = 3) = {
    val d2dReactLines = sc.textFile(s"${dataUrlPrefix}aers/drugs2_reacs_actual_expected")
        .map{ line =>
      val splits = line.split("\\$")
      (Long.parseLong(splits(3)), Double.parseDouble(splits(4)))
    }.filter(x => x._1 >= actualThreshold)

    val binRdd = d2dReactLines.groupBy(x => x._1).flatMap( {
      case (actual, list) => {
        val estimator = new MunroPatersonQuantileEstimator(11)
        val expecteds = new ArrayBuffer[Double]()
        for (item <- list) {
          estimator.add(item._2)
          expecteds.append(item._2)
        }
        val sortedQuantiles = estimator.getQuantiles().sorted
        expecteds.map(expected => (actual, bin(expected, sortedQuantiles), expected))
     }
    })

    val d2dStat = binRdd.groupBy(x => (x._1, x._2)).map( {
      case (key, values) => {
        val size = values.size
        var sum = 0.0D
        for (binItem <- values) sum = (sum + binItem._3)
        key._1 + "," + key._2 + "," + sum + "," + size
      }
    }).saveAsTextFile(s"${dataUrlPrefix}aers/drugs2_reacs_stats")
  }

  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("Step3DrugReactStat")
    sparkConf.set("spark.files.overwrite", "true")
    val sc = new SparkContext(sparkConf)
    drugs2ReacsStat(sc)
    sc.stop()
    println("Done with ADES Step3DrugReactStat!")
  }

}
