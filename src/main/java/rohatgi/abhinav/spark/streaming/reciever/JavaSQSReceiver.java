package rohatgi.abhinav.spark.streaming.reciever;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.util.List;

public class JavaSQSReceiver extends Receiver<String> {

    AmazonSQSClientBuilder amazonSQSClientBuilder = AmazonSQSClientBuilder.standard().withRegion(Regions.DEFAULT_REGION);
    String queueName;
    Credentials credentials;
    Regions region = Regions.DEFAULT_REGION;
    Long timeout = 0L;

    public JavaSQSReceiver(String queueName) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.queueName = queueName;
    }

    public void onStart() {
        new Thread(this::receive).start();
    }

    public void onStop() {
        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself if isStopped() returns false
    }

    private void receive(){
        if(this.credentials!=null){
            this.amazonSQSClientBuilder =  AmazonSQSClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(credentials.getAccessKey(),credentials.getSecretKey())));
        }
        this.amazonSQSClientBuilder.withRegion(this.region);
        final AmazonSQS amazonSQS = this.amazonSQSClientBuilder.build();
        final String sqsQueueUrl = amazonSQS.getQueueUrl(queueName).getQueueUrl();
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsQueueUrl);
        try {
            while (!isStopped()) {
                List<Message> messages = amazonSQS.receiveMessage(receiveMessageRequest).getMessages();
                messages.stream().forEach(m -> {
                    store(m.getBody());
                    amazonSQS.deleteMessage(new DeleteMessageRequest(sqsQueueUrl, m.getReceiptHandle()));
                });
                if (timeout>0L)
                    Thread.sleep(timeout);
            }
            restart("Trying to connect again");
        }catch (IllegalArgumentException e){
            restart("Could not connect", e);
        }catch(Throwable t) {
            restart("Error receiving data", t);
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

    public JavaSQSReceiver withCredentials(String accessKeyId, String secretAccessKey){
        this.credentials = new Credentials(accessKeyId,secretAccessKey);
        return this;
    }

    public  JavaSQSReceiver withCredentials(AWSCredentialsProvider awsCredentialsProvider){
        AWSCredentials awsCredentials = awsCredentialsProvider.getCredentials();
        this.credentials = new Credentials(awsCredentials.getAWSAccessKeyId(),awsCredentials.getAWSSecretKey());
        return this;
    }
}
