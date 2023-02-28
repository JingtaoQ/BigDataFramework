## input the csv from  aws
````
rdd = sc.textFile("s3://s3-de2-jingtao/input/SMS/SMSSpamCollection")
````
## transfer from txt into csv, split by "/t"
````
rdd = rdd.map(lambda line: line.split("\t"))  --是空格
df = rdd.toDF(["class", "message"])
df.head()
`````
![1](https://github.com/JingtaoQ/BigDataFramework/blob/part2/df.show%20csv%20to%20df.png)
## Digital information

Transfer the column into vector

````
indexer = StringIndexer(inputCol="class",outputCol="label",stringOrderType="frequencyDesc")
index_data = indexer.fit(df).transform(df)
--Data classification 0 is ham 1 is spam

token = RegexTokenizer(inputCol="message",outputCol="words",pattern="\\W")

stop = StopWordsRemover(inputCol='words', outputCol='n_words')
--remove the space
````
![remove+label](https://github.com/JingtaoQ/BigDataFramework/blob/part2/remove%20the%20space.png)

creates a numeric dictionary that relates all the words contained in the messages and their relative (to the row) frequencies.

````
countvec = CountVectorizer(inputCol='n_words',outputCol = 'cvec_words')
countvec_data=countvec.fit(stop_data).transform(stop_data)
countvec_data.select('cvec_words').show(1,truncate=False)
````

Usinf IDF to calcute the frequence of each word in the sentence
````
 idf_model = IDF(inputCol = 'cvec_words', outputCol = 'idf_words')
idf_data = idf_model.fit(countvec_data).transform(countvec_data)
idf_data.select('idf_words').show(1,truncate=False)
````

 Each row contains a vector that condenses all the features in a single element. And also get the label.

 `````
assembler = VectorAssembler(inputCols=['idf_words'],outputCol='features')
treated_data = assembler.transform(idf_data)
treated_data.select('features','label').show()
`````
![into features](https://github.com/JingtaoQ/BigDataFramework/blob/part2/vectorAssemble.png)

The code here takes a DataFrame including raw text data and labels as input, and after a series of processing such as indexing, word separation, deactivation, bag-of-words model, IDF calculation, etc., a DataFrame with feature vectors and labels is finally obtained
````
treated_data.select('features')
pipe_prep_data = Pipeline(stages=[indexer, token, stop, countvec,idf_model, assembler])
prep_data = pipe_prep_data.fit(df).transform(df)
prep_data.select('features','label').show(5)
`````
![pipeline](https://github.com/JingtaoQ/BigDataFramework/blob/part2/pipeline.png)

## Train Model

````
train_data, test_data = prep_data.randomSplit([0.8,0.2], seed =1)
train_data.groupBy('label').count().show()
test_data.groupBy('label').count().show()
````
!()[https://github.com/JingtaoQ/BigDataFramework/blob/part2/split%20dataset.png]

Define the accuracy 
`````
def confusion_matrix_stats(predictions, label_col='label', prediction_col='prediction'):
     TP = predictions.filter((col(prediction_col) == 1) & (col(label_col) == 1)).count()
     TN = predictions.filter((col(prediction_col) == 0) & (col(label_col) == 0)).count()
     FP = predictions.filter((col(prediction_col) == 1) & (col(label_col) == 0)).count()
     FN = predictions.filter((col(prediction_col) == 0) & (col(label_col) == 1)).count()
     accuracy = (TP+TN)/(TP+TN+FP+FN)
    print('The accuracy for the test dataset is {}.'.format(accuracy))
`````
### NaiveBayes

 it checks on how much the probability of an element to be one class or the other increases due to the presence of each element in the message. 
````
from pyspark.ml.classification import NaiveBayes

nB = NaiveBayes(featuresCol='features',labelCol='label',modelType='multinomial')

nB_model = nB.fit(train_data)
test_results = nB_model.transform(test_data)                                
test_results.columns
['class', 'message', 'label', 'words', 'n_words', 'cvec_words', 'idf_words', 'features', 'rawPrediction', 'probability', 'prediction']


r4=confusion_matrix_stats(test_results)
````
![navie](https://github.com/JingtaoQ/BigDataFramework/blob/part2/%E6%88%AA%E5%B1%8F2023-02-24%2016.28.38.png)

### RandomForestClassifier
````
from pyspark.ml.classification import RandomForestClassifier as RFC
rfc_model = RFC(featuresCol='features',labelCol='label',maxDepth=10,numTrees=200,impurity='entropy')
r1=confusion_matrix_stats(rfc_test_results)
````
![random](https://github.com/JingtaoQ/BigDataFramework/blob/part2/Random.png)

### LogisticRegression
````
from pyspark.ml.classification import LogisticRegression as LR

lr=LR(labelCol="label",featuresCol="features")
lr_results = lr_model.transform(test_data)
lr_model = lr.fit(train_data)
        # Prediction
        predictions = rfModel.transform(testData)
lr_results = lr_model.transform(test_data)

r2=confusion_matrix_stats(lr_results)
````
![logistic](https://github.com/JingtaoQ/BigDataFramework/blob/part2/LogisticRegression.png)

### DecisionTreeClassifier
````
from pyspark.ml.classification import DecisionTreeClassifier as DTS
dts=DTS(featuresCol='features',labelCol='label',impurity = "entropy",maxDepth = 5,maxBins = 32)
dts_model=dts.fit(train_data)
dts_results=dts_model.transform(test_data)
r3=confusion_matrix_stats(dts_results)
````
![decision](https://github.com/JingtaoQ/BigDataFramework/blob/part2/DecissionTree.png)

### Modity the parameter for the LogiscticRegression
````
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

paramGrid = ParamGridBuilder().addGrid(lr.regParam,[0.1,0.01]).addGrid(lr.fitIntercept,[False,True]).addGrid(lr.elasticNetParam,[0.0, 0.5, 1.0]).build()

lr_update = CrossValidator(estimator=lr,estimatorParamMaps=paramGrid,evaluator=MulticlassClassificationEvaluator())
lr_update_model=lr_update.fit(train_data)
lr_update_results=lr_update_model.transform(test_data)
````
![logistic_update](https://github.com/JingtaoQ/BigDataFramework/blob/part2/LogisticRegression_Update.png)

## comparation
Put the 5 accuracy in a df and compare 
````
r1=confusion_matrix_stats(rfc_test_results)
r2=confusion_matrix_stats(lr_results)
r3=confusion_matrix_stats(dts_results)
r4=confusion_matrix_stats(test_results)
r5=confusion_matrix_stats(lr_update_results)

schema = StructType([
...     StructField("Model", StringType(), True),
...     StructField("Accuracy", FloatType(), True)
... ])
data = [("Random Forest", r1),
...         ("Logistic Regression", r2),
...         ("Decision Tree", r3),
...         ("Naive Bayes", r4),
...         ("Updated Logistic Regression", r5)]
df = spark.createDataFrame(data, schema=schema)
df.show()
````
![compare](https://github.com/JingtaoQ/BigDataFramework/blob/part2/WechatIMG3069.jpeg)




