package worker;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Worker {
	
	public static final String srcBucketName = "mybucket88888888777";
	public static final String dstBucketName = "summary-bucket";
	
	public static final String filePath = "/home/ec2-user/";
	public static String resultFile = "";
	
	public static void main(String[] args) throws IOException, InterruptedException {

        Region region = Region.US_EAST_1;	
        
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();
        
        SqsClient sqsClient = SqsClient.builder()
              .region(region)
                .build();

        String queueInboxURL  = "https://sqs.us-east-1.amazonaws.com/315434705354/InputFifo.fifo";
        String queueOutboxURL = "https://sqs.us-east-1.amazonaws.com/315434705354/OutputFifo.fifo";

        while(true) {
        	
            List<Message> msgs = receiveMessage(sqsClient , queueInboxURL);

        	if (msgs!=null) {
                    String inputFile = msgs.get(0).body();

            		downloadFileFromS3(s3 , inputFile);

            		calculation(new File(inputFile));
            		//uploadFileToS3(s3 , resultFile);
                    
            		/*** Step "6" ***/
                	sendMessage(sqsClient , queueOutboxURL ,  resultFile);
                	
                	//Request was achieved successfully -> Delete the Inbox Msg (Sent by the Client)
                	emptyQueue(sqsClient,queueInboxURL,msgs);
                }
        	
            	System.out.println("Waiting 1 Minute for another Client Request");
            	//Thread.sleep(60000);	//1 minute
        		Thread.sleep(10000);	//10 seconds
        	}    
        
        
    	
	}
	
	public static void downloadFileFromS3(S3Client s3 , String fileName) throws IOException {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(srcBucketName)
                .key(fileName)
                .build();

        Files.deleteIfExists(Paths.get(fileName));	//Delets the file if it already exists
        s3.getObject(getObjectRequest , ResponseTransformer.toFile(Paths.get(fileName)));
        System.out.println("Worker downloaded the sales file from S3 successfully!");
	}

	public static void uploadFileToS3(S3Client s3, String fileName) throws IOException {
    	PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(dstBucketName)
                .key(fileName)
                .build();
    	
    	s3.putObject(objectRequest, RequestBody.fromFile(new File(filePath + fileName)));
        System.out.println("Worker uploaded the result file to S3 successfully!");
	}
	
    public static String createQueue(SqsClient sqsClient,String queueName ) {

        try {
        	
    		Map<QueueAttributeName, String> queueAttributes = new HashMap<>();
            queueAttributes.put(QueueAttributeName.FIFO_QUEUE, "true");
            
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .attributes(queueAttributes)
                .build();

            sqsClient.createQueue(createQueueRequest);

            GetQueueUrlResponse getQueueUrlResponse =
                sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            String queueUrl = getQueueUrlResponse.queueUrl();
            System.out.println(queueName + " Created Successfully!");
            return queueUrl;
        	
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
  
    }
    
    
    public static void sendMessage(SqsClient sqsClient, String queueUrl, String msg) {
    	
    	Long msgId = System.currentTimeMillis();
    	sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(msg)
                .messageGroupId(""+msgId)
                .messageDeduplicationId(""+msgId)
                .build());
    	System.out.println("Message Was Successfully Sent To " + queueUrl);
    	
    }

    public static List<Message> receiveMessage(SqsClient sqsClient, String queueURL) {
        
    	try{
        	ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueURL)
                .maxNumberOfMessages(1)
                .build();
            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
            if(messages.isEmpty()) {
            	System.out.print("Inbox Queue is still empty ");
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
    
	
    
public static void calculation(File inputFile) {
	
	Region region = Region.US_EAST_1;	
    
    S3Client s3 = S3Client.builder()
            .region(region)
            .build();
    
		
	try {
	final BufferedReader reader = new BufferedReader(new FileReader(inputFile));
	//PrintWriter writer = new PrintWriter(resultFile, "UTF-8");

	final DecimalFormat df = new DecimalFormat("0.00");

	String line = "";
	String splitBy = ";";

	HashMap<String, double[]> map = new HashMap<String, double[]>();

	
		reader.readLine();

		double total_quantity = 0;
		double total_profit = 0;
		double total_sold = 0;
		String store = "";
		String timestamp  = "";

		while ((line = reader.readLine()) != null) {
			String[] record = line.split(splitBy);


			timestamp =  record[0];
			store = record[1];
			String key = record[2];

			double q = Integer.valueOf(record[3]);
			double p = Double.valueOf(record[5]);
			double s =  Double.valueOf(record[7]);

			total_quantity += q;
			total_profit += (p * q);
			total_sold += s;

			if (map.containsKey(key)) {
				p = map.get(key)[1] + (p * q);
				q = map.get(key)[0] + q;
				s = map.get(key)[2]+ s;

				double arr[] = { q, p, s };
				map.put(key, arr);

			} else {
				double arr[] = { q, (q * p), s };
				map.put(key, arr);
			}
		}
		reader.close();
		
		String dstKey = timestamp.split(" ")[0].replace('/', '-') +  "/"  + store + ".csv";
		resultFile = dstKey;
		
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter(stringWriter);

		printWriter.println("product/store,quantity,profit,sold");
		printWriter.println(store + "," + Integer.toString((int)total_quantity) + "," +  df.format(total_profit)+ "," +  df.format(total_sold));
		
		
		map.entrySet().forEach(entry -> {
			printWriter.println(entry.getKey() + "," + (int) entry.getValue()[0] + "," + df.format(entry.getValue()[1])+  "," + df.format(entry.getValue()[2]));
			
		});
		
		
		printWriter.close();
		
		PutObjectRequest objectRequest = PutObjectRequest.builder().bucket(dstBucketName).key(dstKey).build();
		s3.putObject(objectRequest, RequestBody.fromString(stringWriter.toString()));
		
		
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		System.out.println("Calculation Done Successfully and the Result Fild was Generated!");
	}
	
	
}
