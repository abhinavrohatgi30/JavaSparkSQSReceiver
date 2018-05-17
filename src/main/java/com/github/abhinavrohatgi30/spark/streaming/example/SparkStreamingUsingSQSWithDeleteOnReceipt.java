package com.github.abhinavrohatgi30.spark.streaming.example;

import com.github.abhinavrohatgi30.spark.streaming.reciever.JavaSQSReceiver;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.amazonaws.regions.Regions;

import scala.Tuple2;

public class SparkStreamingUsingSQSWithDeleteOnReceipt {

	private static Logger logger = Logger.getLogger(SparkStreamingUsingSQSWithDeleteOnReceipt.class);

	public static void main(String[] args) throws InterruptedException {

		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local[2]");
		sparkConf.setAppName("Spark Streaming using SQS");
		String queueName = args[0];
		try (JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(5000))) {
			JavaSQSReceiver javaSQSReceiver = new JavaSQSReceiver(queueName).with(Regions.EU_WEST_1);
			JavaReceiverInputDStream<String> input = jssc.receiverStream(javaSQSReceiver);
			JavaPairDStream<String, Integer> messages = input.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);;
			messages.print();
			jssc.start();
			jssc.awaitTermination();
		} finally {
			logger.info("Exiting the Application");
		}
	}

}
