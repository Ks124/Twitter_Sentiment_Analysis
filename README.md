# Twitter_Sentiment_Analysis
The most challenging task in a Big Data setting is getting the data that can then be used for data analysis and predictions. Towards this goal, I have setup a pipeline to ingest data from Twitter, clean and process it, and load it into a Hive table for analysis. <b>The technologies used are Apache Kafka, Apache Flume for data ingestion into HDFS, and Spark SQL for data analysis and Spark ML for prediction, GCP Cloud.</b>

<b>Step 1: Setup Kafka producer to ingest tweets</b><br>
Setup a Kafka producer in Python that gets data from Twitter for a specific set of keywords related to a topic (the choice of topic and keywords are up to you) and sends it to a topic in a Kafka broker. You will need to sign up for a developer account with Twitter, which is free. The data should be formatted in a way that can be easily ingested by the other components of the pipeline. There is a limit on the number of calls that a producer can make to Twitter at any one time. Check the limitations and adjust your code so that tweets are received continuously without going over the limit. Some sample code is provided for setting up the producer as well online videos.

<b>Step 2: Setup Kafka Consumer</b><br>
Setup a Kafka consumer that reads from the Kafka topic and saves the data to HDFS. The consumer should be designed to handle large volumes of data and should be fault-tolerant. Some sample Kafka consumers are available as well.

<b>Step 3: Setup Flume Agent</b><br>
Apache Flume is a streaming tool typically used for text data. Unlike Apache Kafka, it is more lightweight in installation and setup. Review the videos posted on Apache Flume and setup a Flume agent that gets data from Twitter and saves it to HDFS.

<b>Step 4: Clean and Process Data</b><br>
The data that is saved to HDFS needs to be cleaned and put into multiple columns. It is up to you how you want to clean the data, either in the consumer, producer for Kafka, or in Flume, or at the end of the pipeline. You should ensure that the data is formatted in a way that can be easily loaded into Spark for later processing.

<b>Step 5: Load Data into Spark SQL</b><br>
Data then must be loaded into a Scala DataFrame for analysis. Use the Scala DataFrame to run some queries on the data that you have read. The queries will depend on the topic that you have chosen and keywords received from Twitter.

<b>Step 6: Train a Spark ML algorithm</b><br>
Using the data in HDFS, train a machine learning algorithm using Spark ML to predict whether the tweets that you have have ingested have positive sentiment or negative sentiment. You can also choose other predictions depending on the topic.


