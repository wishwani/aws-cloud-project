package client;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

public class Client {
	
	public static final String srcBucketName = "mybucket88888888777";
	public static final String dstBucketName = "summary-bucket";
	
	public static final String filePath = "/Users/sathish.bowatta/eclipse/2022/Client/sales-data/";
	
	public static final String queueInboxURL = "https://sqs.us-east-1.amazonaws.com/315434705354/InputFifo.fifo";
    public static final String queueOutboxURL = "https://sqs.us-east-1.amazonaws.com/315434705354/OutputFifo.fifo";
    
    
	public static void main(String[] args) throws IOException, InterruptedException {
		
		if (args.length < 1) {
		      System.out.println(
		          "Missing the Input File Name argument");
		      System.exit(1);
		    }
		
	    String inputFile = args[0].toString();
	    Region region = Region.US_EAST_1;   
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();

        SqsClient sqsClient = SqsClient.builder()
                .region(region)
                .build();
        
        uploadFileToS3(s3 , inputFile);	
        
        sendMessage(sqsClient , queueInboxURL ,  inputFile);
    	
        while(true) {
        	List<Message> msgs = receiveMessage(sqsClient , queueOutboxURL);
        	if (msgs!=null) {
            	String resultFile = msgs.get(0).body();
                downloadFileFromS3(s3 , resultFile);
                emptyQueue(sqsClient, queueOutboxURL, msgs);
            	break;
        	}
        	
        	Thread.sleep(10000);	//10 seconds
        }
	}
	
	public static void downloadFileFromS3(S3Client s3 , String fileName) throws IOException {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(dstBucketName)
                .key(fileName)
                .build();

        File dir = new File(fileName);

		if (!dir.exists()) {
			dir.mkdirs();
		}
		Files.deleteIfExists(Paths.get(fileName));
		s3.getObject(getObjectRequest, ResponseTransformer.toFile(Paths.get(fileName)));
		System.out.println("Client downloaded the " + fileName + "from S3 successfully!");
		
		
        Files.deleteIfExists(Paths.get(fileName));	//Delets the file if it already exists
        s3.getObject(getObjectRequest , ResponseTransformer.toFile(Paths.get(fileName)));
        System.out.println("Client downloaded the results file from S3 successfully!");
	}

	public static void uploadFileToS3(S3Client s3, String fileName) throws IOException {
    	PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(srcBucketName)
                .key(fileName)
                .build();
    	
    	s3.putObject(objectRequest, RequestBody.fromFile(new File(filePath + fileName)));
        System.out.println("Client uploaded the result file to S3 successfully!");
        
	}

	public static void sendMessage(SqsClient sqsClient, String queueUrl, String msg) {
	    	
	    	Long msgId = System.currentTimeMillis();
	    	sqsClient.sendMessage(SendMessageRequest.builder()
	                .queueUrl(queueUrl)
	                .messageBody(msg)
	                .messageGroupId(""+msgId)
	                .messageDeduplicationId(""+msgId)
	                .build());
	    	System.out.println("Message was successfully sent by the client to " + queueUrl);
	    	
	    }

	public static List<Message> receiveMessage(SqsClient sqsClient, String queueURL) {
	        
	    	try{
	        	ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
	                .queueUrl(queueURL)
	                .maxNumberOfMessages(1)
	                .build();
	            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
	            if(messages.isEmpty()) {
	            	
	            	return null;
	            }
	            System.out.println("Message: [" + messages.get(0).body() + "] was successfully received from: " + queueURL);
	            return messages;
	            
	        } catch (SqsException e) {
	            System.err.println(e.awsErrorDetails().errorMessage());
	            return null;
	        }
	 }

    public static void emptyQueue(SqsClient sqsClient, String queueURL,  List<Message> messages) {
    	
        try {
            for (Message message : messages) {
                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueURL)
                    .receiptHandle(message.receiptHandle())
                    .build();
                sqsClient.deleteMessage(deleteMessageRequest);
                System.out.println("All messages were deleted successfully from: " + queueURL);
            }
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
   } 
}
