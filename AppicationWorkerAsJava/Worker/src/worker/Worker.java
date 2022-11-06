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
	
	public static final String bucketName = "mybucket88888888777";
	
	public static final String filePath = "/home/ec2-user/";
	public static final String resultFile = "resultFile.txt";
    
    //public static final String queueInboxName = "InputFifo.fifo";
    //public static final String queueOutboxName = "OutputFifo.fifo";
	
	public static void main(String[] args) throws IOException, InterruptedException {

        Region region = Region.US_EAST_1;	
        
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();
        
        SqsClient sqsClient = SqsClient.builder()
              .region(region)
                .build();
        
        //Note: Create the Queues only one time (Run the 2 statements below only once):
        //String queueInboxURL  = createQueue(sqsClient, queueInboxName);
        //String queueOutboxURL = createQueue(sqsClient, queueOutboxName);
        
        //System.out.println(queueInboxURL);	//https://sqs.eu-west-3.amazonaws.com/406165177414/inboxQueue.fifo
        //System.out.println(queueOutboxURL);	//https://sqs.eu-west-3.amazonaws.com/406165177414/outboxQueue.fifo
        
        //If this is not the first run (Queues already created) then use these 2 lines:
        String queueInboxURL  = "https://sqs.us-east-1.amazonaws.com/315434705354/InputFifo.fifo";
        String queueOutboxURL = "https://sqs.us-east-1.amazonaws.com/315434705354/OutputFifo.fifo";
       
        
        while(true) {
        	
            List<Message> msgs = receiveMessage(sqsClient , queueInboxURL);
            
        	if (msgs!=null) {
        		   	
                    /*** Step "3" ***/
                    String inputFile = msgs.get(0).body();
                    
                    /*** Step "4" ***/
            		downloadFileFromS3(s3 , inputFile);

            		/*** Step "5" ***/
            		calculation(new File(inputFile),resultFile);
            		uploadFileToS3(s3 , resultFile);
                    
            		/*** Step "6" ***/
                	sendMessage(sqsClient , queueOutboxURL ,  resultFile);
                	
                	//Request was achieved successfully -> Delete the Inbox Msg (Sent by the Client)
                	emptyQueue(sqsClient,queueInboxURL,msgs);
                }
        	
            	System.out.println("Waiting 1 Minute for another Client Request");
            	//Thread.sleep(60000);	//1 minute
        		Thread.sleep(10000);	//10 seconds
        	}    
        
        //We can also try step by step:

        /*** Step "3" ***/
        //List<Message> msgs = receiveMessage(sqsClient , queueInboxURL);
        //String inputFile = msgs.get(0).body();
        
        /*** Step "4" ***/
        //String inputFile = "sales.csv";
		//downloadFileFromS3(s3 , inputFile);

		/*** Step "5" ***/
		//calculation(new File(inputFile),resultFile);
		//uploadFileToS3(s3 , resultFile);
        
		/*** Step "6" ***/
    	//sendMessage(sqsClient , queueOutboxURL ,  resultFile);
        
        //emptyQueue(sqsClient,queueInboxURL,msgs);
    	
	}
	
	public static void downloadFileFromS3(S3Client s3 , String fileName) throws IOException {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .build();

        Files.deleteIfExists(Paths.get(fileName));	//Delets the file if it already exists
        s3.getObject(getObjectRequest , ResponseTransformer.toFile(Paths.get(fileName)));
        System.out.println("Worker downloaded the sales file from S3 successfully!");
	}

	public static void uploadFileToS3(S3Client s3, String fileName) throws IOException {
    	PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
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
    
	/*public static void calculation(File inputFile, String resultFile) {
		
		String line;
    	int totalNumberOfSales = 0;
    	int totalAmountSold = 0;
    	
    	//HashSet will help us to ignore duplicates.
    	Set<String> productSet = new HashSet<String>();  
    	Set<String> countrySet = new HashSet<String>(); 
    	
    	String[][] table = new String[998][3];
    	
    	List<String> productList;
    	List<String> countryList;
    	
    	HashMap<String , ArrayList<Integer>> avgPerProduct  = new HashMap<String , ArrayList<Integer>>();
    	HashMap<String , ArrayList<Integer>> avgPerCountry  = new HashMap<String , ArrayList<Integer>>();
    	
		try {
			BufferedReader reader = new BufferedReader(new FileReader(inputFile));
			PrintWriter writer = new PrintWriter(resultFile, "UTF-8");

			reader.readLine();	//ignore the header tabs (first line)
	        while((line = reader.readLine()) != null){
	        	
	        	
	        	String[] raw = line.split(",");	//raw:  2:ProductName , 3:Price , 8:Country
	        	totalAmountSold += Integer.parseInt(raw[3]);

	        	productSet.add(raw[2]);
	        	countrySet.add(raw[8]);
	        	
	        	for(int i=0 , j=0 ; i<raw.length ; i++) {		//table:  0:ProductName , 1:Price , 2:Country
	        		if(i == 2 || i == 3 || i == 8) {
	        			table[totalNumberOfSales][j] = raw[i];
	        			j++;
	        		}
	        	}
	        	
	        	totalNumberOfSales++;
	        	
	        	/*if(totalNumberOfSales == 3)
	        		break;*/
	        	
	      /* }
	        
	        //Write totalNumberOfSales and totalAmountSold into the file
			writer.println("Total Number Of Sales: " + totalNumberOfSales);
			writer.println("Total Amount Sold: " + totalAmountSold);
			writer.println();
	        

	        productList = new ArrayList<String>(productSet);
	        Collections.sort(productList);
	        
	        countryList = new ArrayList<String>(countrySet);
	        Collections.sort(countryList);
	        
	        //Creating 2 HashMaps of the form: key -> [priceSum , count]
	        //So that we can use the sum and the count to calculate Average after.
	        for(int i=0 ; i<productList.size() ; i++) {
		        avgPerProduct.put(productList.get(i), new ArrayList<Integer>(Arrays.asList(0,0)));
	        }
	        
	        for(int i=0 ; i<countryList.size() ; i++) {
	        	avgPerCountry.put(countryList.get(i), new ArrayList<Integer>(Arrays.asList(0,0)));
	        }
	        

	        for(int i=0 ; i<table.length ; i++) {	//table:  0:ProductName , 1:Price , 2:Country

	        	int pSum = avgPerProduct.get(table[i][0]).get(0) + Integer.parseInt(table[i][1]);
	        	int pCount = avgPerProduct.get(table[i][0]).get(1) + 1;
	        	avgPerProduct.put(table[i][0], new ArrayList<Integer>(Arrays.asList(pSum,pCount)));
	        	
	        	int cSum = avgPerCountry.get(table[i][2]).get(0) + Integer.parseInt(table[i][1]);
	        	int cCount = avgPerCountry.get(table[i][2]).get(1) + 1;
	        	avgPerCountry.put(table[i][2] , new ArrayList<Integer>(Arrays.asList(cSum,cCount)));	
	        }
	        
	        //System.out.println(avgPerProduct);
	        //System.out.println(avgPerCountry);
	        
	        writer.println("Average Sold Per Product:");
	        
	        for (Map.Entry<String, ArrayList<Integer>> entry : avgPerProduct.entrySet()) {
	            String k = entry.getKey();
	            ArrayList<Integer> v = entry.getValue();
	            
	            //Calculate Average
	            int avg = v.get(0) / v.get(1);
		        v.set(0, avg);
		        
		        //Write in file:
		        writer.println(k + ":  " + v.get(0) + "$" + "  (" + v.get(1) + " item sold)");
	        }
	        
			writer.println();
			writer.println("Average Sold Per Country:");
	        
	        for (Map.Entry<String, ArrayList<Integer>> entry : avgPerCountry.entrySet()) {
	            String k = entry.getKey();
	            ArrayList<Integer> v = entry.getValue();
	            
	          //Calculate Average
	            int avg = v.get(0) / v.get(1);
		        v.set(0, avg);
		        
		      //Write in file:
		        writer.println(k + ":  " + v.get(0) + "$" + "  (" + v.get(1) + " item sold)");
	            
	        }
	        
	        

			reader.close();	
			writer.close();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		System.out.println("Calculation Done Successfully and the Result Fild was Generated!");
	}*/
    
public static void calculation(File inputFile, String resultFile) {
		
	try {
	final BufferedReader reader = new BufferedReader(new FileReader(inputFile));
	PrintWriter writer = new PrintWriter(resultFile, "UTF-8");

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
		
		//String dstKey = timestamp.split(" ")[0].replace('/', '-') +  "/"  + store + ".csv";
		
		reader.close();
       
		
		writer.println("product/store,quantity,profit,sold");
		
		
		writer.println(store + "," + Integer.toString((int)total_quantity) + "," +  df.format(total_profit)+ "," +  df.format(total_sold));
		
		
		map.entrySet().forEach(entry -> {
			writer.println(entry.getKey() + "," + (int) entry.getValue()[0] + "," + df.format(entry.getValue()[1])+  "," + df.format(entry.getValue()[2]));
			
		});
		
		
		writer.close();
		
		
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		System.out.println("Calculation Done Successfully and the Result Fild was Generated!");
	}
	
	
}
