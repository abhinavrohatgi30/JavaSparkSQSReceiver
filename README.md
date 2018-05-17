# JavaSparkSQSReceiver
## An SQS Receiver for Spark Streaming

A custom receiver that lets you stream messages from a SQS queue with the following approaches : 

1. Delete a message on receipt into the receiver 
2. Handle the message deletion from the SQS queue on completion of processing. The latter is achieved by using the ReceiptHandle present as part of the message object which is serialized and sent to spark streaming. 

Refer to the example streaming jobs for both approaches.