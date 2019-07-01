#spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.4.3

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from pyspark.streaming.kafka import KafkaUtils
#from pyspark.sql.functions import lit, unix_timestamp
import time
import datetime
import pandas as pd
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import percent_rank
from pyspark.sql import Window
from pyspark.ml.evaluation import RegressionEvaluator

def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)
def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

def process_rdd_price(time_rdd, rdd):
    #print("----------- %s -----------" % str(time_rdd))
    try:
        
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(price=w[0], timestamp=time_rdd))        
        
        # create a DF from the Row RDD
        price_df = sql_context.createDataFrame(row_rdd)    
                
        # Register the dataframe as table
        price_df.write.saveAsTable("prices", mode = 'append')

    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
        
def process_rdd_tweets(time_rdd, rdd):
    print("----------- %s -----------" % str(time_rdd))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(tweets=w[1], timestamp=time_rdd))
 
        # create a DF from the Row RDD
        tweets_df = sql_context.createDataFrame(row_rdd)

        # Register the dataframe as table
        tweets_df.write.saveAsTable("words", mode = 'append')
        
        joined_df = sql_context.sql("select sum(words.tweets) as num_mentions, avg(prices.price) as avg_price, prices.timestamp from words full outer join prices on words.timestamp = prices.timestamp group by prices.timestamp order by prices.timestamp")
        joined_df.write.saveAsTable("joined_df", mode = 'overwrite')
        
        prediction_df = send_df_to_model(joined_df)
        prediction_df.write.saveAsTable("preds", mode = 'overwrite')
        
        final_df = sql_context.sql("select preds.prediction, joined_df.avg_price, joined_df.num_mentions, joined_df.timestamp from joined_df join preds on joined_df.avg_price = preds.avg_price order by joined_df.timestamp")
        final_df.show()
        
        send_df_to_dashboard(final_df)
        
    except:
        e = sys.exc_info()[0]
        print("Error: %s" %e)

def send_df_to_model(df):
    df = df.withColumn("rank", percent_rank().over(Window.partitionBy().orderBy("timestamp")))
    train_df = df.where("rank <= .5").drop("rank")
    print("Train Set")
    train_df.show()
    test_df = df.where("rank > .5").drop("rank")
    print("Test Set")
    test_df.show()
    vectorAssembler = VectorAssembler(inputCols = ['num_mentions'], outputCol = 'features')
    train_vdf = vectorAssembler.transform(train_df)
    train_vdf = train_vdf.select(['features', 'avg_price'])
    test_vdf = vectorAssembler.transform(test_df)
    test_vdf = test_vdf.select(['features','avg_price'])
    lr = LinearRegression(featuresCol = 'features', labelCol='avg_price', maxIter=10, regParam=0.3, elasticNetParam=0.8)
    lr_model = lr.fit(train_vdf)
    lr_predictions = lr_model.transform(test_vdf)
    
    lr_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="avg_price",metricName="r2")
    print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions))
    test_result = lr_model.evaluate(test_vdf)
    print("Root Mean Squared Error (RMSE) on test data = %g" % test_result.rootMeanSquaredError)
    
    return lr_predictions.select("prediction","avg_price","features")
    
    
def send_df_to_dashboard(df):
    # extract the number of mentions from dataframe and convert them into array
    num_mentions = [t.num_mentions for t in df.select("num_mentions").collect()]
    # extract the prices from dataframe and convert them into array
    avg_price = [p.avg_price for p in df.select("avg_price").collect()]
    # extract the prediction from dataframe and convert them into array
    prediction = [q.prediction for q in df.select("prediction").collect()]
    # extract the timestamps from dataframe and convert them into array
    timestamp = [str(l.timestamp) for l in df.select("timestamp").collect()]
    # initialize and send the data through REST API
    url = 'http://localhost:5001/updateData'
    request_data = {'twitter_data': str(num_mentions), 'bitcoin_data': str(avg_price), 'predicted_data': str(prediction), 'timestamp_label': str(timestamp)}
    response = requests.post(url, data=request_data)
    
# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")

# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 90)

# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# update kafka zookeeper (use this instead of the regular stream)
directKafkaStream_price = KafkaUtils.createDirectStream(ssc, ['bitcoin_vals'], {"bootstrap.servers": 'localhost:9092'})

directKafkaStream_tweets = KafkaUtils.createDirectStream(ssc, ['bitcoin_tweets'], {"bootstrap.servers": 'localhost:9092'})

# split each tweet into words
words_price = directKafkaStream_price.flatMap(lambda line: line[1].split())
words_tweets = directKafkaStream_tweets.flatMap(lambda line: line[1].split())

# Count each word in each batch
pairs_price = words_price.map(lambda word: (word, 1))

pairs_tweets = words_tweets.map(lambda word: (word, 1))
#tweets_totals = pairs_tweets.updateStateByKey(aggregate_tags_count)

#tags_totals.foreachRDD(process_rdd)
pairs_price.foreachRDD(process_rdd_price)

pairs_tweets.foreachRDD(process_rdd_tweets)


#tags_totals.pprint()

# start the streaming computation
ssc.start()

# wait for the streaming to finish
ssc.awaitTermination()
                                            
                                        