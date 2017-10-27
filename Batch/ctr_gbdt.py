from __future__ import print_function
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.util import MLUtils

if __name__ == "__main__":
    sc = SparkContext(appName="CTRGBDTRegression")

    def parsePoint(line):
        line = line.strip("()")
        fields = line.split(',')
        featurs_raw = fields[0:11] # 0 to 10
        features = []
        for x in featurs_raw:
            feature = float(x.strip().strip("'").strip())
            features.append(feature)

        label = float(fields[11])
        #print ("label=" + str(label))
        return LabeledPoint(label,features)

    data = sc.textFile(“./SearchAds/data/log/ctr_features_sample/part*")
    (trainingData, testData) = data.randomSplit([0.7, 0.3])
    parsedTrainData = trainingData.map(parsePoint)
    parsedTestData = testData.map(parsePoint)

    # Train a GradientBoostedTrees model.
    #  Notes: (a) Empty categoricalFeaturesInfo indicates all features are continuous.
    #         (b) Use more iterations in practice.
    model = GradientBoostedTrees.trainClassifier(parsedTrainData,
                                                 categoricalFeaturesInfo={}, numIterations=100)

    # Evaluate model on test instances and compute test error
    predictions = model.predict(parsedTestData.map(lambda x: x.features))
    labelsAndPredictions = parsedTestData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(parsedTestData.count())
    print('training Error = ' + str(testErr))
    print('Learned classification GBT model:')
    print(model.toDebugString())
    print("tree totalNumNodes" + str(model.totalNumNodes()))

    # Save and load model
    model.save(sc, “./SearchAds/data/model/ctr_gbdt_model_demo_small2")
