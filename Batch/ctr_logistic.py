from __future__ import print_function
from pyspark import SparkContext
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils

#(' 101356, 101356, 32714, 32714, 5963, 10594, 21240, 34825, 5963, 7959,1000000', 1)
if __name__ == "__main__":

    sc = SparkContext(appName="CTRLogisticRegression")

    # $example on$
    # Load and parse the data
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

    data = sc.textFile(“./SearchAds/data/log/ctr_features_demo/part*")
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    parsedTrainData = trainingData.map(parsePoint)
    parsedTestData = testData.map(parsePoint)

    # Build the model
    model = LogisticRegressionWithLBFGS.train(parsedTrainData,intercept=False)

    # Evaluating the model on training data
    labelsAndPreds = parsedTestData.map(lambda p: (p.label, model.predict(p.features)))
    trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedTestData.count())
    print("Training Error = " + str(trainErr))
    weights = model.weights
    print("weight= ", weights)
    print("bias=",model.intercept);

    # Save and load model
    model.save(sc, "/Users/jiayangan/project/SearchAds/data/model/ctr_logistic_model_demo4")
    # $example off$
