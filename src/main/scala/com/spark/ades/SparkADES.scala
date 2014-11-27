/**
 * All rights reserved.
 * @author Qiuzhuang.Lian
 */
package com.spark.ades

import com.spark.ades.Step1ETL._
import com.spark.ades.Step2JoinAll._
import com.spark.ades.Step3DrugReactStat._
import com.spark.ades.Step4ApplyEbgm._
import org.apache.spark._

object SparkADES {

  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("SparkADES")
    sparkConf.set("spark.files.overwrite", "true") // to re-process
    val sc = new SparkContext(sparkConf)
    val start = System.currentTimeMillis()
    // step 1
    println("Executing step1 to perform ETL.")
    adeETL(sc)

    // step 2
    println("Executing step2 to calculate expected/actual for drug to drug reaction.")
    genDrug2DrugReacActualAndExpected(sc)

    // step 3
    println("Executing step3 to calculate drug to drug reaction statistics for plugging EBGM algorithm in R.")
    drugs2ReacsStat(sc)

    // step 4
    println("Executing step4 to generate drug to drug scores.")
    applyEbgm(sc)

    sc.stop()
    println(s"Done with ADES, it takes ${(System.currentTimeMillis - start)/1000} seconds!")
  }
}
