package com.github.abhinavrohatgi30.spark.streaming.receiver;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JavaSQSReceiver extends Receiver<String> {

	String queueName;
	private transient AWSCredentialsProvider credentials;
	Regions region = Regions.DEFAULT_REGION;
	Long timeout = 0L;
	ObjectMapper mapper;
	private transient Logger logger;
	boolean deleteOnReceipt;
	
	public JavaSQSReceiver(String queueName) {
		this(queueName,true);
	}
	
	public JavaSQSReceiver(String queueName,boolean deleteOnReceipt) {
		super(StorageLevel.MEMORY_AND_DISK_2());
		this.queueName = queueName;
		this.mapper = new ObjectMapper();
		this.logger = Logger.getLogger(JavaSQSReceiver.class);
		this.deleteOnReceipt = deleteOnReceipt;
	}

	public void onStart() {
		new Thread(this::receive).start();
	}

	public void onStop() {
		// There is nothing much to do as the thread calling receive()
		// is designed to stop by itself if isStopped() returns false
	}

	private void receive() {
		try {
			AmazonSQSClientBuilder amazonSQSClientBuilder = AmazonSQSClientBuilder.standard();
			if (credentials != null) {
				amazonSQSClientBuilder.withCredentials(credentials);
			}
			amazonSQSClientBuilder.withRegion(region);
			final AmazonSQS amazonSQS = amazonSQSClientBuilder.build();
			final String sqsQueueUrl = amazonSQS.getQueueUrl(queueName).getQueueUrl();
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsQueueUrl);
			recieveMessagesFromSQS(amazonSQS, sqsQueueUrl, receiveMessageRequest);
		} catch (Throwable e) {
			stop("Error encountered while initializing", e);
		}
	}

	private void recieveMessagesFromSQS(final AmazonSQS amazonSQS, final String sqsQueueUrl,
			ReceiveMessageRequest receiveMessageRequest) {
		try {
			while (!isStopped()) {
				List<Message> messages = amazonSQS.receiveMessage(receiveMessageRequest).getMessages();
				if (deleteOnReceipt) {
					String recieptHandle = messages.get(0).getReceiptHandle();
					messages.stream().forEach(m -> store(m.getBody()));
					amazonSQS.deleteMessage(new DeleteMessageRequest(sqsQueueUrl, recieptHandle));
				} else {
					messages.stream().forEach(this::storeMessage);
				}
				if (timeout > 0L)
					Thread.sleep(timeout);
			}
			restart("Trying to connect again");
		} catch (IllegalArgumentException | InterruptedException e) {
			restart("Could not connect", e);
		} catch (Throwable e) {
			restart("Error Recieving Data", e);
		}
	}

	private void storeMessage(Message m) {
		try {
			store(mapper.writeValueAsString(m));
		} catch (JsonProcessingException e) {
			logger.error("Unable to write message to streaming context");
		}
	}

	public JavaSQSReceiver withTimeout(Long timeoutInMillis) {
		this.timeout = timeoutInMillis;
		return this;
	}

	public JavaSQSReceiver with(Regions region) {
		this.region = region;
		return this;
	}

	public JavaSQSReceiver withCredentials(String accessKeyId, String secretAccessKey) {
		this.credentials = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretAccessKey));
		return this;
	}

	public JavaSQSReceiver withCredentials(AWSCredentialsProvider awsCredentialsProvider) {
		this.credentials = awsCredentialsProvider;
		return this;
	}
}
