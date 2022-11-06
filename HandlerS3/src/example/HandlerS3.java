package example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.HashMap;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class HandlerS3 implements RequestHandler<S3Event, String> {

	@Override
	public String handleRequest(S3Event event, Context context) {
		LambdaLogger logger = context.getLogger();

		logger.log("[+] Starting WORKER");

		S3Client s3;
		String dstBucketName = "summary-bucket";
		String queueOutboxURL = "https://sqs.us-east-1.amazonaws.com/315434705354/OutputFifo.fifo";

		S3EventNotificationRecord records = event.getRecords().get(0);
		String srcBucketName = records.getS3().getBucket().getName();
		String srcKey = records.getS3().getObject().getUrlDecodedKey();

		Region region = Region.US_EAST_1;
		s3 = S3Client.builder().region(region).build();

		SqsClient sqsClient = SqsClient.builder().region(region).build();

		GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(srcBucketName).key(srcKey).build();

		s3.getObject(getObjectRequest);

		final InputStreamReader streamReader = new InputStreamReader(s3.getObject(getObjectRequest),
				StandardCharsets.UTF_8);
		final BufferedReader reader = new BufferedReader(streamReader);
		final DecimalFormat df = new DecimalFormat("0.00");

		String line = "";
		String splitBy = ";";

		HashMap<String, double[]> map = new HashMap<String, double[]>();

		try {
			reader.readLine();

			double total_quantity = 0;
			double total_profit = 0;
			double total_sold = 0;
			String store = "";
			String timestamp = "";

			while ((line = reader.readLine()) != null) {
				String[] record = line.split(splitBy);

				timestamp = record[0];
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

			logger.log("[+] Completed the file  processing");

			String dstKey = timestamp.split(" ")[0].replace('/', '-') + "/" + store + ".csv";
			logger.log(dstKey);

			StringWriter stringWriter = new StringWriter();
			PrintWriter printWriter = new PrintWriter(stringWriter);


			printWriter.println("product/store,quantity,profit,sold");
			logger.log("product/store,quantity,profit,sold");

			printWriter.println(store + "," + Integer.toString((int)total_quantity) + ","  + df.format(total_profit) + "," + df.format(total_sold));
			logger.log(store + "," + Double.toString(total_quantity) + "," + df.format(total_profit) + "," + df.format(total_sold));

			map.entrySet().forEach(entry -> {
				printWriter.println(entry.getKey() + "," + (int) entry.getValue()[0] + "," +
				df.format(entry.getValue()[1]) +  "," + df.format(entry.getValue()[2]));
				logger.log(entry.getKey() + "," + (int) entry.getValue()[0] + "," + df.format(entry.getValue()[1])+  "," + df.format(entry.getValue()[2]));
			});

			logger.log("[+] End printing data");
			
			PutObjectRequest objectRequest = PutObjectRequest.builder().bucket(dstBucketName).key(dstKey).build();
			s3.putObject(objectRequest, RequestBody.fromString(stringWriter.toString()));

			logger.log("[+] Completed writing to  S3 bucket : " + dstBucketName);

			Long msgId = System.currentTimeMillis();
			sqsClient.sendMessage(SendMessageRequest.builder().queueUrl(queueOutboxURL).messageBody(dstKey)
			.messageGroupId("" + msgId).messageDeduplicationId("" + msgId).build());
			logger.log("[+] Message Was Successfully Sent To " + queueOutboxURL);

		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

}
