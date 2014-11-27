/**
 * All rights reserved.
 * @author Qiuzhuang.Lian
 */
package com.spark.ades

import java.lang._

import com.spark.ades.Model._

import com.cloudera.science.mgps.{NFunction, QFunction}
import org.apache.commons.math.analysis.solvers.BrentSolver
import org.apache.commons.math.analysis.{DifferentiableUnivariateRealFunction, UnivariateRealFunction}
import org.apache.commons.math.analysis.integration.SimpsonIntegrator
import org.apache.commons.math.distribution.{GammaDistributionImpl, GammaDistribution}
import org.apache.spark._

object Step4ApplyEbgm {

  private val nf1 = new NFunction(6.810308e-07, 2.364503e-01)
  private val nf2 = new NFunction(2.499492e-04, 3.896551e+00)
  private val qf = new QFunction(nf1, nf2, 1.000000e+00)

  def ebgm(actual: Int, expected: Double): Double = {
    val qval = qf.eval(actual, expected)
    Math.exp(qval * nf1.delta(actual, expected) + (1.0 - qval) * nf2.delta(actual, expected))
  }

  private val confidenceP = 0.05

  def ebci(actual: Int, expected: Double): Double = {
    val g1 = new GammaDistributionImpl(nf1.getAlpha + actual, nf1.getBeta + expected)
    val g2 = new GammaDistributionImpl(nf2.getAlpha + actual, nf2.getBeta + expected)
    val pi = new PiFunction(qf.eval(actual, expected), g1, g2)
    val ipi = new PiFunctionIntegral(pi, confidenceP)
    try {
      new BrentSolver().solve(ipi, 0.0, 10.0, 0.01)
    } catch {
      case _: Exception => -1.0
    }
  }

  private case class PiFunction(p: Double, g1: GammaDistribution, g2: GammaDistribution)
      extends UnivariateRealFunction {
    override def value(lambda: scala.Double): scala.Double = {
      p * g1.density(lambda) + (1.0 - p) * g2.density(lambda)
    }
  }

  private case class PiFunctionIntegral(pi: PiFunction, target: Double)
      extends DifferentiableUnivariateRealFunction {
    private val integrator = new SimpsonIntegrator()

    override def value(lambda: scala.Double): scala.Double = {
      try {
        if (lambda == 0.0) {
          return -target
        }
        integrator.integrate(pi, 0.0, lambda) - target
      } catch {
        case _: Exception => Double.POSITIVE_INFINITY
      }
    }

    override def derivative(): UnivariateRealFunction = pi
  }

  def applyEbgm(sc: SparkContext, actualThreshold: Int = 3): Unit = {
    val d2dScore = sc.textFile(s"${dataUrlPrefix}aers/drugs2_reacs_actual_expected")
        .map{ line =>
      val splits = line.split("\\$")
      (splits(0), splits(1), splits(2), Integer.parseInt(splits(3)), Double.parseDouble(splits(4)))
    }.filter(x => x._4 >= actualThreshold)
    .map( {
      x => (x._1, x._2, x._3, x._4, x._5, x._4/x._5, ebgm(x._4, x._5), ebci(x._4, x._5))
    })
    .filter(x => x._8 >= 2.0).sortBy(x => x._7, false, 1)
    .map(x => x._1 + "$" + x._2 + "$" + x._3 + "$"+ x._4 + "$"+ x._5 + "$"+ x._6 + "$"+ x._7 + "$"+ x._8)

    d2dScore.saveAsTextFile(s"${dataUrlPrefix}aers/scored_drugs2_reacs")
  }

  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("Step4ApplyEbgm")
    sparkConf.set("spark.files.overwrite", "true")
    val sc = new SparkContext(sparkConf)
    applyEbgm(sc)
    sc.stop()
    println("Done with ADES Step4ApplyEbgm!")
  }

}
