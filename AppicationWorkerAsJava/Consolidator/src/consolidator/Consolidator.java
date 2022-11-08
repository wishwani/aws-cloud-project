package consolidator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;


public class Consolidator {
	
	public static void main(String[] args) throws IOException {
	    Region region = Region.US_EAST_1;

		
		 if (args.length < 1) {
			 System.out.println("Missing the date");
			 System.exit(1); 
		 }
		 

	    String srcBucketName = "summary-bucket";
	    String date = args[0];
	    final DecimalFormat df = new DecimalFormat("0.00");
	    PrintWriter writer = new PrintWriter("/Users/sathish.bowatta/eclipse/2022/Consolidator/finalSummary.txt", "UTF-8");

	    
	    HashMap<String, double[]> map = new HashMap<String, double[]>();
	      
	      double max_profit = 0;
	      double min_profit = -1;
	      double total_p = 0;
	      double total_sold = 0;
	      String max_profit_store  = "";
	      String min_profit_store = "";

	    S3Client s3 = S3Client.builder().region(region).build();
	    

	    ListObjectsRequest listObjects = ListObjectsRequest
                .builder()
                .bucket(srcBucketName)
                .build();

            ListObjectsResponse res = s3.listObjects(listObjects);
            List<S3Object> objects = res.contents();
            for (S3Object myValue : objects) {
            	if (myValue.key().toString().startsWith(date)) {
            		String srcKey = myValue.key();
            		
            		GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(srcBucketName).key(srcKey).build();
        			s3.getObject(getObjectRequest);

        			final InputStreamReader streamReader = new InputStreamReader(s3.getObject(getObjectRequest),
        					StandardCharsets.UTF_8);
        			final BufferedReader reader = new BufferedReader(streamReader);
        			

        			reader.readLine();
        			
        			String[] store_rec = reader.readLine().strip().split(",");

        			double profit = Double.valueOf(store_rec[2]);
        			double sold = Double.valueOf(store_rec[3]);

        			if (max_profit < profit) {
        				max_profit = profit;
        				max_profit_store = store_rec[0];
        			}

        			if (min_profit == -1) {
        				min_profit = profit;
        			}

        			if (min_profit > profit) {
        				min_profit = profit;
        				min_profit_store = store_rec[0];
        			}
        			
        			total_p += profit;
        			total_sold += sold;
        			
        			String line = "";
        			while ((line = reader.readLine()) != null) {
        				String store = "";
        				int q = 0;
        				double p = 0;
        				double s = 0;

        				String[] line_list = line.strip().split(",");

        				store = line_list[0];
        				q = Integer.valueOf(line_list[1]);
        				p = Double.valueOf(line_list[2]);
        				s = Double.valueOf(line_list[3]);

        				if (map.containsKey(store)) {
        					q = (int) map.get(store)[0] + q;
        					p = map.get(store)[1] + p;
        					s = map.get(store)[2] + s;
        				}

        				double arr[] = { q, p, s};
        				map.put(store, arr);
        			}
            	}
            }
            
            writer.println("Summary Results for the application for the date" + date);
            
            writer.println();
            
            
            writer.println("Total Profit: " + total_p +  "," + "Max Profit Store: " +  max_profit_store +  ","  + "Min Profit Store:  " + min_profit_store);
 
            writer.println();
            
            writer.println("Product" + "," + "Total Quantity" + "," + "Total Profit" + "," + "Total Sold");
            
            
            
            map.entrySet().forEach(entry -> {
            	writer.println(entry.getKey() + "," + (int) entry.getValue()[0] + "," +
				df.format(entry.getValue()[1]) +  "," + df.format(entry.getValue()[2]));
				
			});

	  
	    writer.close();
	    
	    System.out.println("Summary Results for the application for the date" + date);
        
	    System.out.println();
        
        
        System.out.println("Total Profit: " + total_p +  "," + "Max Profit Store: " +  max_profit_store +  ","  + "Min Profit Store:  " + min_profit_store);

        System.out.println();
        
        System.out.println("Product" + "," + "Total Quantity" + "," + "Total Profit" + "," + "Total Sold");
        
        
        
        map.entrySet().forEach(entry -> {
        	 System.out.println(entry.getKey() + "," + (int) entry.getValue()[0] + "," +
			df.format(entry.getValue()[1]) +  "," + df.format(entry.getValue()[2]));
			
		});
	    
	    System.out.println("Finish writing the final summary file in the Consolidator");
	      
	      
	  }
}

