/**
 * All rights reserved.
 * @author Qiuzhuang.Lian
 */
package com.spark.ades

import java.lang._

import org.apache.spark._
import org.apache.spark.rdd._

sealed trait Model extends Serializable

/**
 * Model parser with canonical trimming to avoid surprising result.
 *
 * It lets any exception fail elegantly since it uses RDD.filter() later to filter invalid data.
 *
 * It truncates float number data into long for demo's age field.
 */
object Model {

//  val dataUrlPrefix = "file:///data/ade/aers_ascii_2009q4/ascii/"
  val dataUrlPrefix = ""

  import scala.math.Ordered.orderingToOrdered
  /*
  ISR$DRUG_SEQ$ROLE_COD$DRUGNAME$VAL_VBM$ROUTE$DOSE_VBM$DECHAL$RECHAL$LOT_NUM$EXP_DT$NDA_NUM
  6253237$1011824832$PS$ABILIFY$1$INTRAMUSCULAR$$$$$$$
 */
  case class Drug(isr: Long,
                  drugSeq: String,
                  name: String
      ) extends Ordered[Drug] {

    override def compare(that: Drug): Int = isr.compare(that.isr)
  }

  /* "6384984$7099178$F$$6384984-0$$20090922$20091002$EXP$DE-BAYER-200929628GPV$BAYER HEALTHCARE PHARMACEUTICALS INC.$47$YR$F$Y$$$20091002$MD$$$$GERMANY$"
  */
  case class Demo(isr: Long,
                  isrCase: Long,
                  fdaDt: String,
                  age: Long,
                  ageCod: String,
                  gender: String
       ) extends Ordered[Demo] {

    override def compare(that: Demo): Int = isr.compare(that.isr)
  }

  case class React(isr: Long,
                   code: String
      ) extends Ordered[React] {

    override def compare(that: React): Int = isr.compare(that.isr)
  }


  def parseDemo(line: String): Demo = {
    if (line == null || line.length == 0) null
    else {
      try {
        val fields = line.split("\\$")
        val ageStr = fields(11)
        var age = -1L
        var isr = -1L
        var isrCase = -1L
        // Since age field in data source is float compatible so use float to truncate to long.
        age = Float.parseFloat(ageStr.trim).toLong
        isr = Long.parseLong(fields(0).trim)
        isrCase = Long.parseLong(fields(1).trim)
        val fdaDt = fields(7)
        val ageCode = fields(12)
        val gender = fields(13)
        if ("YR".equalsIgnoreCase(ageCode.trim) &&
            ("M".equalsIgnoreCase(gender.trim) || "F".equalsIgnoreCase(gender.trim)) &&
            (fdaDt != null && Integer.parseInt(fdaDt.trim.substring(0, 4)) >= 2008) &&
            (age > 0 && age <= 100)) {
          new Demo(isr, isrCase, fdaDt, age, ageCode, gender)
        } else {
          null
        }
      } catch {
        case e: Throwable =>
          null
      }
    }
  }

  def parseDrug(line: String): Drug = {
    if (line == null || line.length == 0) null
    else {
      val fields = line.split("\\$")
      try {
        new Drug(Long.parseLong(fields(0).trim), fields(1).trim, fields(3).trim)
      } catch {
        case e: Exception =>
          null
      }
    }
  }

  def parseReact(line: String): React = {
    if (line == null || line.length == 0) null
    else {
      val fields = line.split("\\$")
      if (fields.length >= 2) {
        try {
          new React(Long.parseLong(fields(0).trim), fields(1).trim)
        } catch {
          case e: Exception =>
            null
        }
      } else {
        null
      }
    }
  }

  def loadDemo(sc: SparkContext): RDD[Demo] = {
    val demoLines = sc.textFile(s"${dataUrlPrefix}aers/demos")
    println(s"total demo lines ${demoLines.count}")
    val demos = demoLines.map(parseDemo(_)).filter(_ != null)
    demos.groupBy(_.isrCase).map(x => x._2.min)
  }

  def loadDrug(sc: SparkContext): RDD[Drug] = {
    val drugLines = sc.textFile(s"${dataUrlPrefix}aers/drugs")
    drugLines.map(parseDrug(_)).filter(_ != null)
  }

  def loadReact(sc: SparkContext): RDD[React] = {
    val reacLines = sc.textFile(s"${dataUrlPrefix}aers/reactions")
    reacLines.map(parseReact(_)).filter(_ != null)
  }
}
