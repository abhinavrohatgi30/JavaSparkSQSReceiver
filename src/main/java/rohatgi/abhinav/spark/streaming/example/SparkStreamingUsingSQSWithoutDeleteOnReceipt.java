package rohatgi.abhinav.spark.streaming.example;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.databind.ObjectMapper;

import rohatgi.abhinav.spark.streaming.reciever.JavaSQSReceiver;
import scala.Tuple2;

public class SparkStreamingUsingSQSWithoutDeleteOnReceipt {

	private static Logger logger = Logger.getLogger(SparkStreamingUsingSQSWithoutDeleteOnReceipt.class);

	public static void main(String[] args) throws InterruptedException {

		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local");
		sparkConf.setAppName("Spark Streaming using SQS");
		String queueName = args[0];
		try (JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(5000))) {
			JavaSQSReceiver javaSQSReceiver = new JavaSQSReceiver(queueName, false).with(Regions.EU_WEST_1);
			JavaReceiverInputDStream<String> input = jssc.receiverStream(javaSQSReceiver);
			JavaPairDStream<String, Integer> messages = input
					.mapPartitionsToPair(words -> processWords(words,queueName))
					.reduceByKey((a, b) -> a + b);
			messages.print();
			jssc.start();
			jssc.awaitTermination();
		} finally {
			logger.info("Exiting the Application");
		}
	}

	private static Iterator<Tuple2<String, Integer>> processWords(Iterator<String> words,String queueName) {
		List<Tuple2<String, Integer>> tupleList = new ArrayList<>();
		ObjectMapper mapper = new ObjectMapper();
		AmazonSQS amazonSQS = AmazonSQSClientBuilder.standard().withRegion(Regions.EU_WEST_1).build();
		String sqsURL = amazonSQS.getQueueUrl(queueName).getQueueUrl();
		while (words.hasNext()) {
			String word = words.next();
			try {
				Message message = mapper.readValue(word, Message.class);
				tupleList.add(new Tuple2<String, Integer>(message.getBody(), 1));
				amazonSQS.deleteMessage(new DeleteMessageRequest(sqsURL, message.getReceiptHandle()));
			} catch (Exception e) {
				logger.error("Failed to process message", e);
			}
		}
		return tupleList.iterator();
	}
}
