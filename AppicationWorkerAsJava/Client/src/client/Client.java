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
	
	
	public static final String bucketName = "mybucket88888888777";

	
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
	   

        Region region = Region.US_EAST_1;   //Paris
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();

        SqsClient sqsClient = SqsClient.builder()
                .region(region)
                .build();
        

        /*** Step "1" ***/
        uploadFileToS3(s3 , inputFile);	
        
        /*** Step "2" ***/
    	sendMessage(sqsClient , queueInboxURL ,  inputFile);
    	
        while(true) {
        	List<Message> msgs = receiveMessage(sqsClient , queueOutboxURL);
        	if (msgs!=null) {
        		
            	/*** Step "7" ***/
                String resultFile = msgs.get(0).body();
                
                /*** Step "8" ***/
                downloadFileFromS3(s3 , resultFile);
                
                //Received the result -> Delete the Outbox Msg (Sent by the Worker)
        		emptyQueue(sqsClient,queueOutboxURL, msgs);
            	break;
        	}
        	
        	System.out.println("Message is not recieved yet, will waiting again for 1 minute");
           	//Thread.sleep(60000);	//1 minute
        	Thread.sleep(10000);	//10 seconds
        }

        
	}
	
	public static void downloadFileFromS3(S3Client s3 , String fileName) throws IOException {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .build();

        Files.deleteIfExists(Paths.get(fileName));	//Delets the file if it already exists
        s3.getObject(getObjectRequest , ResponseTransformer.toFile(Paths.get(fileName)));
        System.out.println("Client downloaded the results file from S3 successfully!");
	}

	public static void uploadFileToS3(S3Client s3, String fileName) throws IOException {
    	PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
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
	    	System.out.println("Message Was Successfully Sent By the Client To " + queueUrl);
	    	
	    }

	public static List<Message> receiveMessage(SqsClient sqsClient, String queueURL) {
	        
	    	try{
	        	ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
	                .queueUrl(queueURL)
	                .maxNumberOfMessages(1)
	                .build();
	            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
	            if(messages.isEmpty()) {
	            	System.out.print("Outbox Queue is still empty ");
	            	return null;
	            }
	            System.out.println("Message: [" + messages.get(0).body() + "] Was Successfully Received from: " + queueURL);
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
                System.out.println("All Messages Were Deleted Successfully from: " + queueURL);
            }
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
   } 
}
