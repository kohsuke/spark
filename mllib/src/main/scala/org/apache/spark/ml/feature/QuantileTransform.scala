/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.feature

import java.{util => ju}

import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util.QuantileSummaries
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Params for [[QuantileTransform]].
 */
private[feature] trait QuantileTransformParams extends Params
  with HasInputCol with HasOutputCol with HasRelativeError {

  /**
   * Number of quantiles to be computed. It corresponds to the number of landmarks used
   * to discretize the cumulative distribution function.
   * Default: 1000
   * @group param
   */
  val numQuantiles: IntParam = new IntParam(this, "numQuantiles",
    "Number of quantiles to be computed", ParamValidators.gt(2))

  /** @group getParam */
  def getNumQuantiles: Int = $(numQuantiles)

  setDefault(numQuantiles -> 1000)

  /**
   * Marginal distribution for the transformed data.
   * Supported options: "uniform", and "gaussian".
   * (default = uniform)
   * @group param
   */
  val distribution: Param[String] = new Param(this, "distribution",
    "Marginal distribution for the transformed data.",
    ParamValidators.inArray[String](QuantileTransform.supportedDistributions))

  /** @group getParam */
  def getDistribution: String = $(distribution)

  setDefault(distribution -> "uniform")

  /**
   * Whether the zero entries of the input are discarded to compute the quantile statistics.
   * If True, the zero-valued entries of the vectors are discarded to compute the quantile
   * statistics. If False, these entries are taken into account.
   * Default: false
   *
   * @group param
   */
  val skipZero: BooleanParam = new BooleanParam(this, "skipZero",
    "Whether the zero entries of the input are discarded to compute the quantile statistics")

  /** @group getParam */
  def getSkipZero: Boolean = $(skipZero)

  setDefault(skipZero -> false)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    SchemaUtils.appendColumn(schema, StructField($(outputCol), new VectorUDT, false))
  }
}

/**
 * QuantileTransform provide a non-parametric transformation to map the data to another
 * distribution, currently both uniform and gaussian are supported.
 * This model transforms the features to follow a uniform or a gaussian distribution.
 * Therefore, for a given feature, this transformation tends to spread out the most frequent
 * values. It also reduces the impact of (marginal) outliers: this is therefore a robust
 * preprocessing scheme.
 * The transformation is applied on each feature independently. First an estimate of the
 * cumulative distribution function of a feature is used to map the original values to a
 * uniform distribution. The obtained values are then mapped to the desired output distribution
 * using the associated quantile function.
 */
class QuantileTransform(override val uid: String)
  extends Estimator[QuantileTransformModel] with QuantileTransformParams
    with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("quan_trans"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group expertSetParam */
  def setRelativeError(value: Double): this.type = set(relativeError, value)

  /** @group setParam */
  def setNumQuantiles(value: Int): this.type = set(numQuantiles, value)

  /** @group setParam */
  def setDistribution(value: String): this.type = set(distribution, value)

  /** @group setParam */
  def setSkipZero(value: Boolean): this.type = set(skipZero, value)

  override def fit(dataset: Dataset[_]): QuantileTransformModel = {
    transformSchema(dataset.schema, logging = true)

    val n = $(numQuantiles)
    val localRelativeError = $(relativeError)
    val localSkipZero = $(skipZero)

    val vectors = dataset.select($(inputCol)).rdd.map {
      case Row(vec: Vector) => vec
    }
    val numFeatures = vectors.first().size

    val collected = vectors.flatMap { vec =>
      require(vec.size == numFeatures,
        s"Number of dimensions must be $numFeatures but got ${vec.size}")
      if (localSkipZero) {
        val activeIter = vec match {
          case DenseVector(values) =>
            Iterator.range(0, numFeatures).map { i => (i, values(i)) }
          case SparseVector(_, indices, values) =>
            Iterator.range(0, indices.length).map { i => (indices(i), values(i)) }
        }
        activeIter.filter { case (_, v) => v != 0 && !v.isNaN }
      } else {
        Iterator.range(0, numFeatures)
          .map(i => (i, vec(i)))
          .filter(!_._2.isNaN)
      }
    }.aggregateByKey(
      new QuantileSummaries(QuantileSummaries.defaultCompressThreshold, localRelativeError))(
      seqOp = (s, v) => s.insert(v),
      combOp = (s1, s2) => s1.compress.merge(s2.compress)
    ).mapValues { s =>
      // confirm compression before query
      val s2 = s.compress
      val q = Array.tabulate(n)(i => s2.query(i.toDouble / (n - 1)).get)
      Vectors.dense(q)
    }.collect().sortBy(_._1)
    require(collected.length == numFeatures,
      "QuantileSummaries on some dimensions were not computed")

    val quantiles = collected.map(_._2)
    copyValues(new QuantileTransformModel(uid, quantiles)
      .setParent(this))
  }

  override def copy(extra: ParamMap): QuantileTransform =
    defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}


@Since("3.0.0")
object QuantileTransform extends DefaultParamsReadable[QuantileTransform] {

  /** String name for uniform distribution. */
  private[feature] val Uniform: String = "uniform"

  /** String name for gaussian distribution. */
  private[feature] val Gaussian: String = "gaussian"

  /* Set of distributions that QuantileTransformer supports */
  private[feature] val supportedDistributions = Array(Uniform, Gaussian)

  private[feature] val BOUNDS_THRESHOLD = 1e-7

  override def load(path: String): QuantileTransform = super.load(path)
}


/**
 * Model fitted by [[QuantileTransform]].
 * @param quantiles The values corresponding the quantiles of reference.
 */
@Since("3.0.0")
class QuantileTransformModel private[ml] (
    override val uid: String,
    val quantiles: Array[Vector])
  extends Model[QuantileTransformModel] with QuantileTransformParams with MLWritable {
  require(quantiles.forall(_.size == quantiles.head.size))

  import QuantileTransform._
  import QuantileTransformModel._

  val numFeatures: Int = quantiles.length

  // Quantiles of references.
  lazy val references: Vector = {
    val n = $(numQuantiles)
    Vectors.dense(Array.tabulate(n)(_.toDouble / (n - 1)))
  }

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = validateAndTransformSchema(schema)
    if ($(outputCol).nonEmpty) {
      outputSchema = SchemaUtils.updateAttributeGroupSize(outputSchema,
        $(outputCol), numFeatures)
    }
    outputSchema
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    val func = $(distribution) match {
      case Uniform => getUniformTransformFunc
      case Gaussian => getGaussianTransformFunc
    }

    val transformer = udf { vec: Vector => require(vec.size == numFeatures); func(vec) }
    dataset.withColumn($(outputCol), transformer(col($(inputCol))),
      outputSchema($(outputCol)).metadata)
  }

  private def getUniformTransformFunc: Vector => Vector = {
    def compute(i: Int, v: Double): Double = {
      if (v.isNaN) {
        Double.NaN
      } else {
        interpolate(quantiles(i).toArray, references.toArray, v)
      }
    }

    if ($(skipZero)) {
      case dv: DenseVector =>
        val values = Array.ofDim[Double](dv.size)
        dv.foreachActive { (i, v) =>
          if (v != 0) values(i) = compute(i, v)
        }
        Vectors.dense(values)

      case sv: SparseVector =>
        val values = Array.ofDim[Double](sv.numActives)
        var j = 0
        sv.foreachActive { (i, v) =>
          if (v != 0) values(j) = compute(i, v)
          j += 1
        }
        Vectors.sparse(sv.size, sv.indices.clone(), values)
    } else {
      vec: Vector =>
        val values = Array.ofDim[Double](vec.size)
        var i = 0
        while (i < vec.size) {
          val v = vec(i)
          values(i) = compute(i, v)
          i += 1
        }
        Vectors.dense(values)
    }
  }

  private def getGaussianTransformFunc: Vector => Vector = {
    val gaussian = new NormalDistribution(0, 1)
    val clipMin = gaussian.inverseCumulativeProbability(BOUNDS_THRESHOLD - MLUtils.EPSILON)
    val clipMax = gaussian.inverseCumulativeProbability(1 - (BOUNDS_THRESHOLD - MLUtils.EPSILON))

    def compute(i: Int, v: Double): Double = {
      if (v.isNaN) {
        Double.NaN
      } else {
        val q = quantiles(i).toArray
        if (v - BOUNDS_THRESHOLD < q.head) {
          clipMin
        } else if (v + BOUNDS_THRESHOLD > q.last) {
          clipMax
        } else {
          val p = interpolate(q, references.toArray, v)
          gaussian.inverseCumulativeProbability(p)
        }
      }
    }

    if ($(skipZero)) {
      case dv: DenseVector =>
        val values = Array.ofDim[Double](dv.size)
        dv.foreachActive { (i, v) =>
          if (v != 0) values(i) = compute(i, v)
        }
        Vectors.dense(values)

      case sv: SparseVector =>
        val values = Array.ofDim[Double](sv.numActives)
        var j = 0
        sv.foreachActive { (i, v) =>
          if (v != 0) values(j) = compute(i, v)
          j += 1
        }
        Vectors.sparse(sv.size, sv.indices.clone(), values)
    } else {
      vec: Vector =>
        val values = Array.ofDim[Double](vec.size)
        var i = 0
        while (i < vec.size) {
          val v = vec(i)
          values(i) = compute(i, v)
          i += 1
        }
        Vectors.dense(values)
    }
  }

  /**
   * One-dimensional linear interpolation.
   * @param xp x-coordinates of the data points, must be increasing.
   * @param yp y-coordinates of the data points.
   * @param x input x-coordinate of the interpolated values.
   * @return
   */
  private def interpolate(xp: Array[Double], yp: Array[Double], x: Double): Double = {
    val j = ju.Arrays.binarySearch(xp, x)
    if (j < 0) {
      val i = -j - 1
      if (i == 0) {
        yp.head
      } else if (i == xp.length) {
        yp.last
      } else {
        val x1 = xp(i - 1)
        val x2 = xp(i)
        val y1 = yp(i - 1)
        val y2 = yp(i)
        y1 + (y2 - y1) * (x - x1) / (x2 - x1)
      }
    } else {
      yp(j)
    }
  }

  override def copy(extra: ParamMap): QuantileTransformModel = {
    val copied = new QuantileTransformModel(uid, quantiles)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new QuantileTransformModelWriter(this)

  override def toString: String = {
    s"QuantileTransformModel: uid=$uid, distribution=${$(distribution)}, " +
      s"numQuantiles=${$(numQuantiles)}, numFeatures=$numFeatures"
  }
}



@Since("3.0.0")
object QuantileTransformModel extends MLReadable[QuantileTransformModel] {

  private[QuantileTransformModel]
  class QuantileTransformModelWriter(instance: QuantileTransformModel) extends MLWriter {

    private case class Data(index: Int, quantile: Vector)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = instance.quantiles.toSeq.zipWithIndex
        .map { case (q, i) => Data(i, q) }
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(data).write.parquet(dataPath)
    }
  }

  private class QuantileTransformModelReader extends MLReader[QuantileTransformModel] {

    private val className = classOf[QuantileTransformModel].getName

    override def load(path: String): QuantileTransformModel = {
      val spark = sparkSession
      import spark.implicits._

      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString

      val quantiles = sparkSession.read.parquet(dataPath)
        .select("index", "quantile")
        .map { row =>
          val i = row.getInt(0)
          val q = row.getAs[Vector](1)
          (i, q)
        }.collect()
        .sortBy(_._1)
        .map(_._2)

      val model = new QuantileTransformModel(metadata.uid, quantiles)
      metadata.getAndSetParams(model)
      model
    }
  }

  override def read: MLReader[QuantileTransformModel] = new QuantileTransformModelReader

  override def load(path: String): QuantileTransformModel = super.load(path)
}
