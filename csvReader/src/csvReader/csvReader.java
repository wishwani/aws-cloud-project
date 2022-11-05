package csvReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.core.sync.RequestBody;

import java.text.DecimalFormat;

public class csvReader implements RequestHandler<S3Event, String> {

	private static S3Client s3;
	private static String dstBucketName  = "summarybuck";

	@Override
	public String handleRequest(S3Event event, Context context) {

		
		LambdaLogger logger = context.getLogger();
		S3EventNotificationRecord records = event.getRecords().get(0);
		String srcBucketName = records.getS3().getBucket().getName();
		String srcKey = records.getS3().getObject().getUrlDecodedKey();

		Region region = Region.US_EAST_1;
		s3 = S3Client.builder().region(region).build();

		GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(srcBucketName).key(srcKey).build();

		s3.getObject(getObjectRequest);

		final InputStreamReader streamReader = new InputStreamReader(s3.getObject(getObjectRequest), StandardCharsets.UTF_8);
		final BufferedReader reader = new BufferedReader(streamReader);
		final DecimalFormat df = new DecimalFormat("0.00");

		String line = "";
		String splitBy = ";";

		HashMap<String, double[]> map = new HashMap<String, double[]>();

		try {
			reader.readLine();

			double total_quantity = 0;
			double total_profit = 0;
			String store = "";
			String timestamp  = "";

			while ((line = reader.readLine()) != null) {
				String[] record = line.split(splitBy);


				timestamp =  record[0];
				store = record[1];
				String key = record[2];

				double q = Integer.valueOf(record[3]);
				double p = Double.valueOf(record[5]);

				total_quantity += q;
				total_profit += (p * q);

				if (map.containsKey(key)) {
					p = map.get(key)[1] + (p * q);
					q = map.get(key)[0] + q;

					double arr[] = { q, p };
					map.put(key, arr);

				} else {
					double arr[] = { q, (q * p) };
					map.put(key, arr);
				}
			}
			
			String dstKey = timestamp.split(" ")[0].replace('/', '-') +  "/"  + store + ".csv";
	        logger.log(dstKey);
			
			StringWriter stringWriter = new  StringWriter();
			PrintWriter printWriter = new PrintWriter(stringWriter);
			
			printWriter.println("product/store,quantity,profit");
			logger.log("product/store,quantity,profit");
			
			printWriter.println(store + "," + Integer.toString((int)total_quantity) + "," +  df.format(total_profit));
			logger.log(store + "," + Double.toString(total_quantity) + "," +  df.format(total_profit));
			
			map.entrySet().forEach(entry -> {
				printWriter.println(entry.getKey() + "," + (int) entry.getValue()[0] + "," + df.format(entry.getValue()[1]));
				logger.log(entry.getKey() + "," + (int) entry.getValue()[0] + "," + df.format(entry.getValue()[1]));
			});
			
			
			PutObjectRequest objectRequest = PutObjectRequest.builder().bucket(dstBucketName).key(dstKey).build();
			PutObjectResponse response = s3.putObject(objectRequest, RequestBody.fromString(stringWriter.toString()));
			
			logger.log("[+] Completed writing to  S3 bucket : " + dstBucketName );
			logger.log(response.toString());

			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "OK";
	}
}