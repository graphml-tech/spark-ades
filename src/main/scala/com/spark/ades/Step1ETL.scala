/**
 * All rights reserved.
 *
 * @author Qiuzhuang.Lian
 */
package com.spark.ades

import java.lang._

import com.spark.ades.Model._
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd._

import scala.collection.mutable.ArrayBuffer

object Step1ETL {
  /**
   * Join demo, drug and react via isr
   */
  def jointDemoDrugReact(sc: SparkContext, demoRdd: RDD[Demo], drugRdd: RDD[Drug], reactRdd: RDD[React]):
      RDD[(Long, Demo, Drug, React)] = {
    val demoPair = demoRdd.map(x => (x.isr, x))
    val drugPair = drugRdd.map(x => (x.isr, x))
    val reactPair = reactRdd.map(x => (x.isr, x))
    val join = demoPair.join(drugPair).join(reactPair)
    join.map( {
      case (k, ((demo, drug), react)) => (k, demo, drug, react)
    })
  }

  /**
   * Join demo, drug, drug and react via isr, choose2 function in Pig
   */
  def joinDrug2Drug(sc: SparkContext, demoRdd: RDD[Demo], drugRdd: RDD[Drug], reactRdd: RDD[React]):
      RDD[(Long, Demo, Drug, Drug, React)] = {
    val drug2DrugPairs = drugRdd.groupBy( {
      case (drug) => drug.isr
    }).map({
      case (k, drugs) => (k, {
        val drugBuffer = new ArrayBuffer[Drug]()
        val drugNames = new ArrayBuffer[String]()
        for (item <- drugs) {
          if (!drugNames.contains(item.name)) {
            drugNames.append(item.name)
            drugBuffer.append(item)
          }
        }
        drugBuffer.toSeq
      })
    }).filter( {
      case (k, drugs) => drugs.size > 1
    }).flatMap( {
      case (k, drugs) => pairDrug2Drug(drugs)
    }).map( {
      case (drug1, drug2) => (drug1.isr, (drug1, drug2))
    })
    val demoPair = demoRdd.map(x => (x.isr, x))
    val reactPair = reactRdd.map(x => (x.isr, x))
    val finalRdd = demoPair.join(reactPair).join(drug2DrugPairs).map( {
      case (k, ((demo, react), (drug1, drug2))) => (k, demo, drug1, drug2, react)
    })
    finalRdd
  }

  def demoCounts(joinRdd: RDD[(Long, Demo, Drug, React)]): RDD[((String, Long, String), Int)] = {
    joinRdd.groupBy( {
      case (id, demo, drug, react) => (demo.gender, demo.age, demo.fdaDt)
    }).filter( {
      case (k, list) => !list.isEmpty
    }).map( {
      case (group, list) => (group, {
        val isrs = list.map( {
          case (id, demo, drug, react) => demo.isr
        })
        isrs.toSeq.distinct.size
      })
    })
  }

  def drugCounts(joinRdd: RDD[(Long, Demo, Drug, React)]): RDD[((String, Long, String, String), Int)] = {
    joinRdd.groupBy( {
      case (id, demo, drug, react) => (demo.gender, demo.age, demo.fdaDt, drug.name)
    }).filter({
      case (k, list) => !list.isEmpty
    }).map({
      case (group, list) => (group, {
        val isrs = list.map( {
          case (id, demo, drug, react) => demo.isr
        })
        isrs.toSeq.distinct.size
      })
    })

  }

  def reactCounts(joinRdd: RDD[(Long, Demo, Drug, React)]): RDD[((String, Long, String, String), Int)] = {
    joinRdd.groupBy( {
      case (id, demo, drug, react) => (demo.gender, demo.age, demo.fdaDt, react.code)
    }).filter({
      case (k, list) => !list.isEmpty
    }).map( {
      case (group, list) => (group, {
        val isrs = list.map( {
          case (id, demo, drug, react) => demo.isr
        })
        isrs.toSeq.distinct.size
      })
    })
  }

  def drugReactCounts(joinRdd: RDD[(Long, Demo, Drug, React)]):
    RDD[((String, Long, String, String, String), Int)] = {
    joinRdd.groupBy( {
      case (id, demo, drug, react) => (demo.gender, demo.age, demo.fdaDt, drug.name, react.code)
    }).filter( {
      case (k, list) => !list.isEmpty
    }).map( {
      case (group, list) => (group, {
        val isrs = list.map( {
          case(id, demo, drug, react) => demo.isr
        })
        isrs.toSeq.distinct.size
      })
    })
  }

  def drug2DrugCounts(join2Rdd: RDD[(Long, Demo, Drug, Drug, React)]):
    RDD[((String, Long, String, String, String, String), Int)] = {
    join2Rdd.groupBy( {
      case (id, demo, drug1, drug2, react) =>
        (demo.gender, demo.age, demo.fdaDt, drug1.name, drug2.name, react.code)
    }).filter( {
      case (k, list) => !list.isEmpty
    }).map( {
      case (group, list) => (group, {
        val isrs = list.map( {
          case (id, demo, drug1,drug2, react) => demo.isr
        })
        isrs.toSeq.distinct.size
      })
    })
  }

  /**
   * This generates the 2 arity drug pair based on drug list.
   */
  private[this] def pairDrug2Drug(drugsNotOrder: Seq[Drug]): Seq[(Drug, Drug)] = {
    val pairs = new ArrayBuffer[(Drug, Drug)]()
    var i = 0
    var j = 0
    val drugs = drugsNotOrder.sortBy(_.name)
    val size = drugs.size
    for (i <- 0 until size) {
      val pivot = i+1
      j = pivot
      if (pivot < size) {
        for (j <- pivot until size) {
          // since we had already sorted the drug list.
          pairs += ((drugs(i), drugs(j)))
        }
      }
    }
    pairs
  }

  def adeETL(sc: SparkContext) {
    val demoRdd = loadDemo(sc)
    val drugRdd = loadDrug(sc)
    val reactRdd = loadReact(sc)
    val join1Rdd: RDD[(Long, Demo, Drug, React)] = jointDemoDrugReact(sc, demoRdd, drugRdd, reactRdd)
    join1Rdd.cache()

    join1Rdd.map( {
      case (id, demo, drug, react) =>
        drug.name + "$" + react.code + "$" + demo.isr + "$" +
            demo.gender + "$" + demo.age + "$" + demo.fdaDt
    }).saveAsTextFile(s"${dataUrlPrefix}aers/strat_drugs1_reacs")

    val demoCountsRdd: RDD[((String, Long, String), Int)] = demoCounts(join1Rdd)
    demoCountsRdd.map( {
      case ((gender, age, fdaDt), count) => gender + "$" + age + "$" + fdaDt + "$" + count
    }).saveAsTextFile(s"${dataUrlPrefix}aers/strat_demo_counts")

    val drugCountsRdd: RDD[((String, Long, String, String), Int)] = drugCounts(join1Rdd)
    drugCountsRdd.map( {
      case ((gender, age, fdaDt, drug), count) =>
        gender + "$" + age + "$" + fdaDt + "$" + drug + "$" + count
    }).saveAsTextFile(s"${dataUrlPrefix}aers/strat_drugs_counts")

    val reactCountsRdd: RDD[((String, Long, String, String), Int)] = reactCounts(join1Rdd)
    reactCountsRdd.map( {
      case ((gender, age, fdaDt, react), count) =>
        gender + "$" + age + "$" + fdaDt + "$" + react + "$" + count
    }).saveAsTextFile(s"${dataUrlPrefix}aers/strat_reacs_counts")

    val drugReactCountsRdd: RDD[((String, Long, String, String, String), Int)] = drugReactCounts(join1Rdd)
    drugReactCountsRdd.map( {
      case ((gender, age, fdaDt, drug, react), count) =>
        gender + "$" + age + "$" + fdaDt + "$" + drug + "$" + react + "$" + count
    }).saveAsTextFile(s"${dataUrlPrefix}aers/strat_drugs_reacs_counts")

    join1Rdd.unpersist(true)

    val join2Rdd: RDD[(Long, Demo, Drug, Drug, React)] = joinDrug2Drug(sc, demoRdd, drugRdd, reactRdd)
    join2Rdd.map( {
      case (key, demo, drug1, drug2, react) =>
        drug1.name + "$" + drug2.name + "$" + react.code + "$" + demo.isr + "$" +
            demo.gender + "$" + demo.age + "$" + demo.fdaDt
    }).saveAsTextFile(s"${dataUrlPrefix}aers/strat_drugs2_reacs")

    val drug2CountsRdd: RDD[((String, Long, String, String, String, String), Int)] = drug2DrugCounts(join2Rdd)
    drug2CountsRdd.map( {
      case ((gender, age, fdaDt, drug1, drug2, react), count) =>
        gender + "$" + age + "$" + fdaDt + "$" + drug1 + "$" + drug2 + "$" + react + "$" + count
    }).saveAsTextFile(s"${dataUrlPrefix}aers/strat_drugs2_reacs_counts")
  }

  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("Step1ETL")
    val sc = new SparkContext(sparkConf)
    adeETL(sc)
    sc.stop()
    println("Done with ADES Step1ETL!")
  }
}

