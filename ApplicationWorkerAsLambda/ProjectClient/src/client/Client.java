package client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class Client {

	public static final String bucketName = "uploadbucket-worker";
	public static final String queueOutboxURL = "https://sqs.us-east-1.amazonaws.com/315434705354/OutputFifo.fifo";

	public static void main(String[] args) throws IOException, InterruptedException {
		Region region = Region.US_EAST_1;

		if (args.length < 1) {
		      System.out.println(
		          "Missing the File Name argument");
		      System.exit(1);
		    }
		

	    String filepath = args[0].toString();
	    String  [] namelist  = filepath.split("/");
	    String filename =  namelist[namelist.length-1];
		

		SqsClient sqsClient = SqsClient.builder().region(region).build();

		S3Client s3 = S3Client.builder().region(region).build();

		ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
		ListBucketsResponse listBucketResponse = s3.listBuckets(listBucketsRequest);

		if ((listBucketResponse.hasBuckets())
				&& (listBucketResponse.buckets().stream().noneMatch(x -> x.name().equals(bucketName)))) {

			CreateBucketRequest bucketRequest = CreateBucketRequest.builder().bucket(bucketName).build();

			s3.createBucket(bucketRequest);
		}

		PutObjectRequest putOb = PutObjectRequest.builder().bucket(bucketName).key(filename).build();
		s3.putObject(putOb, RequestBody.fromBytes(getObjectFile(filepath)));

		while (true) {
			List<Message> msgs = receiveMessage(sqsClient, queueOutboxURL);
			if (msgs != null) {

				String resultFile = msgs.get(0).body();

				downloadFileFromS3(s3, resultFile, "summary-bucket");

				// Received the result -> Delete the Outbox Msg (Sent by the Worker)
				emptyQueue(sqsClient, queueOutboxURL, msgs);
				break;
			}
			Thread.sleep(10000); // 10 seconds
		}
	}

	private static byte[] getObjectFile(String filePath) {

		FileInputStream fileInputStream = null;
		byte[] bytesArray = null;

		try {
			File file = new File(filePath);
			bytesArray = new byte[(int) file.length()];
			fileInputStream = new FileInputStream(file);
			fileInputStream.read(bytesArray);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fileInputStream != null) {
				try {
					fileInputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return bytesArray;
	}

	public static void downloadFileFromS3(S3Client s3, String fileName, String bucketName) throws IOException {
		GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(fileName).build();

		File dir = new File(fileName);

		if (!dir.exists()) {
			dir.mkdirs();
		}
		Files.deleteIfExists(Paths.get(fileName));
		s3.getObject(getObjectRequest, ResponseTransformer.toFile(Paths.get(fileName)));
		System.out.println("Client downloaded the " + fileName + "from S3 successfully!");

	}

	public static List<Message> receiveMessage(SqsClient sqsClient, String queueURL) {

		try {
			ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(queueURL)
					.maxNumberOfMessages(1).build();
			List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
			if (messages.isEmpty()) {
				
				return null;
			}
			System.out.println("Message: [" + messages.get(0).body() + "] Was Successfully Received from: " + queueURL);
			return messages;

		} catch (SqsException e) {
			System.err.println(e.awsErrorDetails().errorMessage());
			return null;
		}
	}

	public static void emptyQueue(SqsClient sqsClient, String queueURL, List<Message> messages) {

		try {
			for (Message message : messages) {
				DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(queueURL)
						.receiptHandle(message.receiptHandle()).build();
				sqsClient.deleteMessage(deleteMessageRequest);
				System.out.println("All Messages Were Deleted Successfully from: " + queueURL);
			}
		} catch (SqsException e) {
			System.err.println(e.awsErrorDetails().errorMessage());
			System.exit(1);
		}
	}

}
