#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
from abc import abstractmethod, ABCMeta

from pyspark import since, keyword_only
from pyspark.ml.wrapper import JavaParams
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.param.shared import HasLabelCol, HasPredictionCol, HasProbabilityCol, \
    HasRawPredictionCol, HasFeaturesCol, HasWeightCol
from pyspark.ml.common import inherit_doc
from pyspark.ml.util import JavaMLReadable, JavaMLWritable

__all__ = ['Evaluator', 'BinaryClassificationEvaluator', 'RegressionEvaluator',
           'MulticlassClassificationEvaluator', 'MultilabelClassificationEvaluator',
           'ClusteringEvaluator', 'RankingEvaluator']


@inherit_doc
class Evaluator(Params):
    """
    Base class for evaluators that compute metrics from predictions.

    .. versionadded:: 1.4.0
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def _evaluate(self, dataset):
        """
        Evaluates the output.

        :param dataset: a dataset that contains labels/observations and
               predictions
        :return: metric
        """
        raise NotImplementedError()

    @since("1.4.0")
    def evaluate(self, dataset, params=None):
        """
        Evaluates the output with optional parameters.

        :param dataset: a dataset that contains labels/observations and
                        predictions
        :param params: an optional param map that overrides embedded
                       params
        :return: metric
        """
        if params is None:
            params = dict()
        if isinstance(params, dict):
            if params:
                return self.copy(params)._evaluate(dataset)
            else:
                return self._evaluate(dataset)
        else:
            raise ValueError("Params must be a param map but got %s." % type(params))

    @since("1.5.0")
    def isLargerBetter(self):
        """
        Indicates whether the metric returned by :py:meth:`evaluate` should be maximized
        (True, default) or minimized (False).
        A given evaluator may support multiple metrics which may be maximized or minimized.
        """
        return True


@inherit_doc
class JavaEvaluator(JavaParams, Evaluator):
    """
    Base class for :py:class:`Evaluator`s that wrap Java/Scala
    implementations.
    """

    __metaclass__ = ABCMeta

    def _evaluate(self, dataset):
        """
        Evaluates the output.
        :param dataset: a dataset that contains labels/observations and predictions.
        :return: evaluation metric
        """
        self._transfer_params_to_java()
        return self._java_obj.evaluate(dataset._jdf)

    def isLargerBetter(self):
        self._transfer_params_to_java()
        return self._java_obj.isLargerBetter()


@inherit_doc
class BinaryClassificationEvaluator(JavaEvaluator, HasLabelCol, HasRawPredictionCol, HasWeightCol,
                                    JavaMLReadable, JavaMLWritable):
    """
    Evaluator for binary classification, which expects two input columns: rawPrediction and label.
    The rawPrediction column can be of type double (binary 0/1 prediction, or probability of label
    1) or of type vector (length-2 vector of raw predictions, scores, or label probabilities).

    >>> from pyspark.ml.linalg import Vectors
    >>> scoreAndLabels = map(lambda x: (Vectors.dense([1.0 - x[0], x[0]]), x[1]),
    ...    [(0.1, 0.0), (0.1, 1.0), (0.4, 0.0), (0.6, 0.0), (0.6, 1.0), (0.6, 1.0), (0.8, 1.0)])
    >>> dataset = spark.createDataFrame(scoreAndLabels, ["raw", "label"])
    ...
    >>> evaluator = BinaryClassificationEvaluator()
    >>> evaluator.setRawPredictionCol("raw")
    BinaryClassificationEvaluator...
    >>> evaluator.evaluate(dataset)
    0.70...
    >>> evaluator.evaluate(dataset, {evaluator.metricName: "areaUnderPR"})
    0.83...
    >>> bce_path = temp_path + "/bce"
    >>> evaluator.save(bce_path)
    >>> evaluator2 = BinaryClassificationEvaluator.load(bce_path)
    >>> str(evaluator2.getRawPredictionCol())
    'raw'
    >>> scoreAndLabelsAndWeight = map(lambda x: (Vectors.dense([1.0 - x[0], x[0]]), x[1], x[2]),
    ...    [(0.1, 0.0, 1.0), (0.1, 1.0, 0.9), (0.4, 0.0, 0.7), (0.6, 0.0, 0.9),
    ...     (0.6, 1.0, 1.0), (0.6, 1.0, 0.3), (0.8, 1.0, 1.0)])
    >>> dataset = spark.createDataFrame(scoreAndLabelsAndWeight, ["raw", "label", "weight"])
    ...
    >>> evaluator = BinaryClassificationEvaluator(rawPredictionCol="raw", weightCol="weight")
    >>> evaluator.evaluate(dataset)
    0.70...
    >>> evaluator.evaluate(dataset, {evaluator.metricName: "areaUnderPR"})
    0.82...
    >>> evaluator.getNumBins()
    1000

    .. versionadded:: 1.4.0
    """

    metricName = Param(Params._dummy(), "metricName",
                       "metric name in evaluation (areaUnderROC|areaUnderPR)",
                       typeConverter=TypeConverters.toString)

    numBins = Param(Params._dummy(), "numBins", "Number of bins to down-sample the curves "
                    "(ROC curve, PR curve) in area computation. If 0, no down-sampling will "
                    "occur. Must be >= 0.",
                    typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, rawPredictionCol="rawPrediction", labelCol="label",
                 metricName="areaUnderROC", weightCol=None, numBins=1000):
        """
        __init__(self, rawPredictionCol="rawPrediction", labelCol="label", \
                 metricName="areaUnderROC", weightCol=None, numBins=1000)
        """
        super(BinaryClassificationEvaluator, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.evaluation.BinaryClassificationEvaluator", self.uid)
        self._setDefault(metricName="areaUnderROC", numBins=1000)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @since("1.4.0")
    def setMetricName(self, value):
        """
        Sets the value of :py:attr:`metricName`.
        """
        return self._set(metricName=value)

    @since("1.4.0")
    def getMetricName(self):
        """
        Gets the value of metricName or its default value.
        """
        return self.getOrDefault(self.metricName)

    @since("3.0.0")
    def setNumBins(self, value):
        """
        Sets the value of :py:attr:`numBins`.
        """
        return self._set(numBins=value)

    @since("3.0.0")
    def getNumBins(self):
        """
        Gets the value of numBins or its default value.
        """
        return self.getOrDefault(self.numBins)

    def setLabelCol(self, value):
        """
        Sets the value of :py:attr:`labelCol`.
        """
        return self._set(labelCol=value)

    def setRawPredictionCol(self, value):
        """
        Sets the value of :py:attr:`rawPredictionCol`.
        """
        return self._set(rawPredictionCol=value)

    @since("3.0.0")
    def setWeightCol(self, value):
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)

    @keyword_only
    @since("1.4.0")
    def setParams(self, rawPredictionCol="rawPrediction", labelCol="label",
                  metricName="areaUnderROC", weightCol=None, numBins=1000):
        """
        setParams(self, rawPredictionCol="rawPrediction", labelCol="label", \
                  metricName="areaUnderROC", weightCol=None, numBins=1000)
        Sets params for binary classification evaluator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)


@inherit_doc
class RegressionEvaluator(JavaEvaluator, HasLabelCol, HasPredictionCol, HasWeightCol,
                          JavaMLReadable, JavaMLWritable):
    """
    Evaluator for Regression, which expects input columns prediction, label
    and an optional weight column.

    >>> scoreAndLabels = [(-28.98343821, -27.0), (20.21491975, 21.5),
    ...   (-25.98418959, -22.0), (30.69731842, 33.0), (74.69283752, 71.0)]
    >>> dataset = spark.createDataFrame(scoreAndLabels, ["raw", "label"])
    ...
    >>> evaluator = RegressionEvaluator()
    >>> evaluator.setPredictionCol("raw")
    RegressionEvaluator...
    >>> evaluator.evaluate(dataset)
    2.842...
    >>> evaluator.evaluate(dataset, {evaluator.metricName: "r2"})
    0.993...
    >>> evaluator.evaluate(dataset, {evaluator.metricName: "mae"})
    2.649...
    >>> re_path = temp_path + "/re"
    >>> evaluator.save(re_path)
    >>> evaluator2 = RegressionEvaluator.load(re_path)
    >>> str(evaluator2.getPredictionCol())
    'raw'
    >>> scoreAndLabelsAndWeight = [(-28.98343821, -27.0, 1.0), (20.21491975, 21.5, 0.8),
    ...   (-25.98418959, -22.0, 1.0), (30.69731842, 33.0, 0.6), (74.69283752, 71.0, 0.2)]
    >>> dataset = spark.createDataFrame(scoreAndLabelsAndWeight, ["raw", "label", "weight"])
    ...
    >>> evaluator = RegressionEvaluator(predictionCol="raw", weightCol="weight")
    >>> evaluator.evaluate(dataset)
    2.740...
    >>> evaluator.getThroughOrigin()
    False

    .. versionadded:: 1.4.0
    """
    metricName = Param(Params._dummy(), "metricName",
                       """metric name in evaluation - one of:
                       rmse - root mean squared error (default)
                       mse - mean squared error
                       r2 - r^2 metric
                       mae - mean absolute error
                       var - explained variance.""",
                       typeConverter=TypeConverters.toString)

    throughOrigin = Param(Params._dummy(), "throughOrigin",
                          "whether the regression is through the origin.",
                          typeConverter=TypeConverters.toBoolean)

    @keyword_only
    def __init__(self, predictionCol="prediction", labelCol="label",
                 metricName="rmse", weightCol=None, throughOrigin=False):
        """
        __init__(self, predictionCol="prediction", labelCol="label", \
                 metricName="rmse", weightCol=None, throughOrigin=False)
        """
        super(RegressionEvaluator, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.evaluation.RegressionEvaluator", self.uid)
        self._setDefault(metricName="rmse", throughOrigin=False)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @since("1.4.0")
    def setMetricName(self, value):
        """
        Sets the value of :py:attr:`metricName`.
        """
        return self._set(metricName=value)

    @since("1.4.0")
    def getMetricName(self):
        """
        Gets the value of metricName or its default value.
        """
        return self.getOrDefault(self.metricName)

    @since("3.0.0")
    def setThroughOrigin(self, value):
        """
        Sets the value of :py:attr:`throughOrigin`.
        """
        return self._set(throughOrigin=value)

    @since("3.0.0")
    def getThroughOrigin(self):
        """
        Gets the value of throughOrigin or its default value.
        """
        return self.getOrDefault(self.throughOrigin)

    def setLabelCol(self, value):
        """
        Sets the value of :py:attr:`labelCol`.
        """
        return self._set(labelCol=value)

    def setPredictionCol(self, value):
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    @since("3.0.0")
    def setWeightCol(self, value):
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)

    @keyword_only
    @since("1.4.0")
    def setParams(self, predictionCol="prediction", labelCol="label",
                  metricName="rmse", weightCol=None, throughOrigin=False):
        """
        setParams(self, predictionCol="prediction", labelCol="label", \
                  metricName="rmse", weightCol=None, throughOrigin=False)
        Sets params for regression evaluator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)


@inherit_doc
class MulticlassClassificationEvaluator(JavaEvaluator, HasLabelCol, HasPredictionCol, HasWeightCol,
                                        HasProbabilityCol, JavaMLReadable, JavaMLWritable):
    """
    Evaluator for Multiclass Classification, which expects input
    columns: prediction, label, weight (optional) and probabilityCol (only for logLoss).

    >>> scoreAndLabels = [(0.0, 0.0), (0.0, 1.0), (0.0, 0.0),
    ...     (1.0, 0.0), (1.0, 1.0), (1.0, 1.0), (1.0, 1.0), (2.0, 2.0), (2.0, 0.0)]
    >>> dataset = spark.createDataFrame(scoreAndLabels, ["prediction", "label"])
    >>> evaluator = MulticlassClassificationEvaluator()
    >>> evaluator.setPredictionCol("prediction")
    MulticlassClassificationEvaluator...
    >>> evaluator.evaluate(dataset)
    0.66...
    >>> evaluator.evaluate(dataset, {evaluator.metricName: "accuracy"})
    0.66...
    >>> evaluator.evaluate(dataset, {evaluator.metricName: "truePositiveRateByLabel",
    ...     evaluator.metricLabel: 1.0})
    0.75...
    >>> mce_path = temp_path + "/mce"
    >>> evaluator.save(mce_path)
    >>> evaluator2 = MulticlassClassificationEvaluator.load(mce_path)
    >>> str(evaluator2.getPredictionCol())
    'prediction'
    >>> scoreAndLabelsAndWeight = [(0.0, 0.0, 1.0), (0.0, 1.0, 1.0), (0.0, 0.0, 1.0),
    ...     (1.0, 0.0, 1.0), (1.0, 1.0, 1.0), (1.0, 1.0, 1.0), (1.0, 1.0, 1.0),
    ...     (2.0, 2.0, 1.0), (2.0, 0.0, 1.0)]
    >>> dataset = spark.createDataFrame(scoreAndLabelsAndWeight, ["prediction", "label", "weight"])
    >>> evaluator = MulticlassClassificationEvaluator(predictionCol="prediction",
    ...     weightCol="weight")
    >>> evaluator.evaluate(dataset)
    0.66...
    >>> evaluator.evaluate(dataset, {evaluator.metricName: "accuracy"})
    0.66...
    >>> predictionAndLabelsWithProbabilities = [
    ...      (1.0, 1.0, 1.0, [0.1, 0.8, 0.1]), (0.0, 2.0, 1.0, [0.9, 0.05, 0.05]),
    ...      (0.0, 0.0, 1.0, [0.8, 0.2, 0.0]), (1.0, 1.0, 1.0, [0.3, 0.65, 0.05])]
    >>> dataset = spark.createDataFrame(predictionAndLabelsWithProbabilities, ["prediction",
    ...     "label", "weight", "probability"])
    >>> evaluator = MulticlassClassificationEvaluator(predictionCol="prediction",
    ...     probabilityCol="probability")
    >>> evaluator.setMetricName("logLoss")
    MulticlassClassificationEvaluator...
    >>> evaluator.evaluate(dataset)
    0.9682...

    .. versionadded:: 1.5.0
    """
    metricName = Param(Params._dummy(), "metricName",
                       "metric name in evaluation "
                       "(f1|accuracy|weightedPrecision|weightedRecall|weightedTruePositiveRate|"
                       "weightedFalsePositiveRate|weightedFMeasure|truePositiveRateByLabel|"
                       "falsePositiveRateByLabel|precisionByLabel|recallByLabel|fMeasureByLabel|"
                       "logLoss)",
                       typeConverter=TypeConverters.toString)
    metricLabel = Param(Params._dummy(), "metricLabel",
                        "The class whose metric will be computed in truePositiveRateByLabel|"
                        "falsePositiveRateByLabel|precisionByLabel|recallByLabel|fMeasureByLabel."
                        " Must be >= 0. The default value is 0.",
                        typeConverter=TypeConverters.toFloat)
    beta = Param(Params._dummy(), "beta",
                 "The beta value used in weightedFMeasure|fMeasureByLabel."
                 " Must be > 0. The default value is 1.",
                 typeConverter=TypeConverters.toFloat)
    eps = Param(Params._dummy(), "eps",
                "log-loss is undefined for p=0 or p=1, so probabilities are clipped to "
                "max(eps, min(1 - eps, p)). "
                "Must be in range (0, 0.5). The default value is 1e-15.",
                typeConverter=TypeConverters.toFloat)

    @keyword_only
    def __init__(self, predictionCol="prediction", labelCol="label",
                 metricName="f1", weightCol=None, metricLabel=0.0, beta=1.0,
                 probabilityCol="probability", eps=1e-15):
        """
        __init__(self, predictionCol="prediction", labelCol="label", \
                 metricName="f1", weightCol=None, metricLabel=0.0, beta=1.0, \
                 probabilityCol="probability", eps=1e-15)
        """
        super(MulticlassClassificationEvaluator, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator", self.uid)
        self._setDefault(metricName="f1", metricLabel=0.0, beta=1.0, eps=1e-15)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @since("1.5.0")
    def setMetricName(self, value):
        """
        Sets the value of :py:attr:`metricName`.
        """
        return self._set(metricName=value)

    @since("1.5.0")
    def getMetricName(self):
        """
        Gets the value of metricName or its default value.
        """
        return self.getOrDefault(self.metricName)

    @since("3.0.0")
    def setMetricLabel(self, value):
        """
        Sets the value of :py:attr:`metricLabel`.
        """
        return self._set(metricLabel=value)

    @since("3.0.0")
    def getMetricLabel(self):
        """
        Gets the value of metricLabel or its default value.
        """
        return self.getOrDefault(self.metricLabel)

    @since("3.0.0")
    def setBeta(self, value):
        """
        Sets the value of :py:attr:`beta`.
        """
        return self._set(beta=value)

    @since("3.0.0")
    def getBeta(self):
        """
        Gets the value of beta or its default value.
        """
        return self.getOrDefault(self.beta)

    @since("3.0.0")
    def setEps(self, value):
        """
        Sets the value of :py:attr:`eps`.
        """
        return self._set(eps=value)

    @since("3.0.0")
    def getEps(self):
        """
        Gets the value of eps or its default value.
        """
        return self.getOrDefault(self.eps)

    def setLabelCol(self, value):
        """
        Sets the value of :py:attr:`labelCol`.
        """
        return self._set(labelCol=value)

    def setPredictionCol(self, value):
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    @since("3.0.0")
    def setProbabilityCol(self, value):
        """
        Sets the value of :py:attr:`probabilityCol`.
        """
        return self._set(probabilityCol=value)

    @since("3.0.0")
    def setWeightCol(self, value):
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)

    @keyword_only
    @since("1.5.0")
    def setParams(self, predictionCol="prediction", labelCol="label",
                  metricName="f1", weightCol=None, metricLabel=0.0, beta=1.0,
                  probabilityCol="probability", eps=1e-15):
        """
        setParams(self, predictionCol="prediction", labelCol="label", \
                  metricName="f1", weightCol=None, metricLabel=0.0, beta=1.0, \
                  probabilityCol="probability", eps=1e-15)
        Sets params for multiclass classification evaluator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)


@inherit_doc
class MultilabelClassificationEvaluator(JavaEvaluator, HasLabelCol, HasPredictionCol,
                                        JavaMLReadable, JavaMLWritable):
    """
    .. note:: Experimental

    Evaluator for Multilabel Classification, which expects two input
    columns: prediction and label.

    >>> scoreAndLabels = [([0.0, 1.0], [0.0, 2.0]), ([0.0, 2.0], [0.0, 1.0]),
    ...     ([], [0.0]), ([2.0], [2.0]), ([2.0, 0.0], [2.0, 0.0]),
    ...     ([0.0, 1.0, 2.0], [0.0, 1.0]), ([1.0], [1.0, 2.0])]
    >>> dataset = spark.createDataFrame(scoreAndLabels, ["prediction", "label"])
    ...
    >>> evaluator = MultilabelClassificationEvaluator()
    >>> evaluator.setPredictionCol("prediction")
    MultilabelClassificationEvaluator...
    >>> evaluator.evaluate(dataset)
    0.63...
    >>> evaluator.evaluate(dataset, {evaluator.metricName: "accuracy"})
    0.54...
    >>> mlce_path = temp_path + "/mlce"
    >>> evaluator.save(mlce_path)
    >>> evaluator2 = MultilabelClassificationEvaluator.load(mlce_path)
    >>> str(evaluator2.getPredictionCol())
    'prediction'

    .. versionadded:: 3.0.0
    """
    metricName = Param(Params._dummy(), "metricName",
                       "metric name in evaluation "
                       "(subsetAccuracy|accuracy|hammingLoss|precision|recall|f1Measure|"
                       "precisionByLabel|recallByLabel|f1MeasureByLabel|microPrecision|"
                       "microRecall|microF1Measure)",
                       typeConverter=TypeConverters.toString)
    metricLabel = Param(Params._dummy(), "metricLabel",
                        "The class whose metric will be computed in precisionByLabel|"
                        "recallByLabel|f1MeasureByLabel. "
                        "Must be >= 0. The default value is 0.",
                        typeConverter=TypeConverters.toFloat)

    @keyword_only
    def __init__(self, predictionCol="prediction", labelCol="label",
                 metricName="f1Measure", metricLabel=0.0):
        """
        __init__(self, predictionCol="prediction", labelCol="label", \
                 metricName="f1Measure", metricLabel=0.0)
        """
        super(MultilabelClassificationEvaluator, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.evaluation.MultilabelClassificationEvaluator", self.uid)
        self._setDefault(metricName="f1Measure", metricLabel=0.0)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @since("3.0.0")
    def setMetricName(self, value):
        """
        Sets the value of :py:attr:`metricName`.
        """
        return self._set(metricName=value)

    @since("3.0.0")
    def getMetricName(self):
        """
        Gets the value of metricName or its default value.
        """
        return self.getOrDefault(self.metricName)

    @since("3.0.0")
    def setMetricLabel(self, value):
        """
        Sets the value of :py:attr:`metricLabel`.
        """
        return self._set(metricLabel=value)

    @since("3.0.0")
    def getMetricLabel(self):
        """
        Gets the value of metricLabel or its default value.
        """
        return self.getOrDefault(self.metricLabel)

    @since("3.0.0")
    def setLabelCol(self, value):
        """
        Sets the value of :py:attr:`labelCol`.
        """
        return self._set(labelCol=value)

    @since("3.0.0")
    def setPredictionCol(self, value):
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    @keyword_only
    @since("3.0.0")
    def setParams(self, predictionCol="prediction", labelCol="label",
                  metricName="f1Measure", metricLabel=0.0):
        """
        setParams(self, predictionCol="prediction", labelCol="label", \
                  metricName="f1Measure", metricLabel=0.0)
        Sets params for multilabel classification evaluator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)


@inherit_doc
class ClusteringEvaluator(JavaEvaluator, HasPredictionCol, HasFeaturesCol,
                          JavaMLReadable, JavaMLWritable):
    """
    Evaluator for Clustering results, which expects two input
    columns: prediction and features. The metric computes the Silhouette
    measure using the squared Euclidean distance.

    The Silhouette is a measure for the validation of the consistency
    within clusters. It ranges between 1 and -1, where a value close to
    1 means that the points in a cluster are close to the other points
    in the same cluster and far from the points of the other clusters.

    >>> from pyspark.ml.linalg import Vectors
    >>> featureAndPredictions = map(lambda x: (Vectors.dense(x[0]), x[1]),
    ...     [([0.0, 0.5], 0.0), ([0.5, 0.0], 0.0), ([10.0, 11.0], 1.0),
    ...     ([10.5, 11.5], 1.0), ([1.0, 1.0], 0.0), ([8.0, 6.0], 1.0)])
    >>> dataset = spark.createDataFrame(featureAndPredictions, ["features", "prediction"])
    ...
    >>> evaluator = ClusteringEvaluator()
    >>> evaluator.setPredictionCol("prediction")
    ClusteringEvaluator...
    >>> evaluator.evaluate(dataset)
    0.9079...
    >>> ce_path = temp_path + "/ce"
    >>> evaluator.save(ce_path)
    >>> evaluator2 = ClusteringEvaluator.load(ce_path)
    >>> str(evaluator2.getPredictionCol())
    'prediction'

    .. versionadded:: 2.3.0
    """
    metricName = Param(Params._dummy(), "metricName",
                       "metric name in evaluation (silhouette)",
                       typeConverter=TypeConverters.toString)
    distanceMeasure = Param(Params._dummy(), "distanceMeasure", "The distance measure. " +
                            "Supported options: 'squaredEuclidean' and 'cosine'.",
                            typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, predictionCol="prediction", featuresCol="features",
                 metricName="silhouette", distanceMeasure="squaredEuclidean"):
        """
        __init__(self, predictionCol="prediction", featuresCol="features", \
                 metricName="silhouette", distanceMeasure="squaredEuclidean")
        """
        super(ClusteringEvaluator, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.evaluation.ClusteringEvaluator", self.uid)
        self._setDefault(metricName="silhouette", distanceMeasure="squaredEuclidean")
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @keyword_only
    @since("2.3.0")
    def setParams(self, predictionCol="prediction", featuresCol="features",
                  metricName="silhouette", distanceMeasure="squaredEuclidean"):
        """
        setParams(self, predictionCol="prediction", featuresCol="features", \
                  metricName="silhouette", distanceMeasure="squaredEuclidean")
        Sets params for clustering evaluator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.3.0")
    def setMetricName(self, value):
        """
        Sets the value of :py:attr:`metricName`.
        """
        return self._set(metricName=value)

    @since("2.3.0")
    def getMetricName(self):
        """
        Gets the value of metricName or its default value.
        """
        return self.getOrDefault(self.metricName)

    @since("2.4.0")
    def setDistanceMeasure(self, value):
        """
        Sets the value of :py:attr:`distanceMeasure`.
        """
        return self._set(distanceMeasure=value)

    @since("2.4.0")
    def getDistanceMeasure(self):
        """
        Gets the value of `distanceMeasure`
        """
        return self.getOrDefault(self.distanceMeasure)

    def setFeaturesCol(self, value):
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    def setPredictionCol(self, value):
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)


@inherit_doc
class RankingEvaluator(JavaEvaluator, HasLabelCol, HasPredictionCol,
                       JavaMLReadable, JavaMLWritable):
    """
    .. note:: Experimental

    Evaluator for Ranking, which expects two input
    columns: prediction and label.

    >>> scoreAndLabels = [([1.0, 6.0, 2.0, 7.0, 8.0, 3.0, 9.0, 10.0, 4.0, 5.0],
    ...     [1.0, 2.0, 3.0, 4.0, 5.0]),
    ...     ([4.0, 1.0, 5.0, 6.0, 2.0, 7.0, 3.0, 8.0, 9.0, 10.0], [1.0, 2.0, 3.0]),
    ...     ([1.0, 2.0, 3.0, 4.0, 5.0], [])]
    >>> dataset = spark.createDataFrame(scoreAndLabels, ["prediction", "label"])
    ...
    >>> evaluator = RankingEvaluator()
    >>> evaluator.setPredictionCol("prediction")
    RankingEvaluator...
    >>> evaluator.evaluate(dataset)
    0.35...
    >>> evaluator.evaluate(dataset, {evaluator.metricName: "precisionAtK", evaluator.k: 2})
    0.33...
    >>> ranke_path = temp_path + "/ranke"
    >>> evaluator.save(ranke_path)
    >>> evaluator2 = RankingEvaluator.load(ranke_path)
    >>> str(evaluator2.getPredictionCol())
    'prediction'

    .. versionadded:: 3.0.0
    """
    metricName = Param(Params._dummy(), "metricName",
                       "metric name in evaluation "
                       "(meanAveragePrecision|meanAveragePrecisionAtK|"
                       "precisionAtK|ndcgAtK|recallAtK)",
                       typeConverter=TypeConverters.toString)
    k = Param(Params._dummy(), "k",
              "The ranking position value used in meanAveragePrecisionAtK|precisionAtK|"
              "ndcgAtK|recallAtK. Must be > 0. The default value is 10.",
              typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, predictionCol="prediction", labelCol="label",
                 metricName="meanAveragePrecision", k=10):
        """
        __init__(self, predictionCol="prediction", labelCol="label", \
                 metricName="meanAveragePrecision", k=10)
        """
        super(RankingEvaluator, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.evaluation.RankingEvaluator", self.uid)
        self._setDefault(metricName="meanAveragePrecision", k=10)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @since("3.0.0")
    def setMetricName(self, value):
        """
        Sets the value of :py:attr:`metricName`.
        """
        return self._set(metricName=value)

    @since("3.0.0")
    def getMetricName(self):
        """
        Gets the value of metricName or its default value.
        """
        return self.getOrDefault(self.metricName)

    @since("3.0.0")
    def setK(self, value):
        """
        Sets the value of :py:attr:`k`.
        """
        return self._set(k=value)

    @since("3.0.0")
    def getK(self):
        """
        Gets the value of k or its default value.
        """
        return self.getOrDefault(self.k)

    @since("3.0.0")
    def setLabelCol(self, value):
        """
        Sets the value of :py:attr:`labelCol`.
        """
        return self._set(labelCol=value)

    @since("3.0.0")
    def setPredictionCol(self, value):
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    @keyword_only
    @since("3.0.0")
    def setParams(self, predictionCol="prediction", labelCol="label",
                  metricName="meanAveragePrecision", k=10):
        """
        setParams(self, predictionCol="prediction", labelCol="label", \
                  metricName="meanAveragePrecision", k=10)
        Sets params for ranking evaluator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)


if __name__ == "__main__":
    import doctest
    import tempfile
    import pyspark.ml.evaluation
    from pyspark.sql import SparkSession
    globs = pyspark.ml.evaluation.__dict__.copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    spark = SparkSession.builder\
        .master("local[2]")\
        .appName("ml.evaluation tests")\
        .getOrCreate()
    globs['spark'] = spark
    temp_path = tempfile.mkdtemp()
    globs['temp_path'] = temp_path
    try:
        (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
        spark.stop()
    finally:
        from shutil import rmtree
        try:
            rmtree(temp_path)
        except OSError:
            pass
    if failure_count:
        sys.exit(-1)
