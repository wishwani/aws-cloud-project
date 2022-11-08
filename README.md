# aws-cloud-project
AWS

# Team Members:

- RATHWATHTHAGE Randika Wishwani Rathwaththa
- SIVARATHNAM Pachava

# To Run The Project:

We have created 2 separate projects according to the worker application (Java and Lambda)

 
**1. Client, Worker as a Lambda Function and Consolidator**

1- Create 3 Java maven projects for the client(name:ProjectClient), the worker(name:HandlerS3), the consolidator(name:Consolidator).

2- Create: 2 S3 buckets(name:uploadbucket-worker, name:summary-bucket) and 1 Fifo queues(name:OutputFifo.fifo), Lambda function(name:workerFunction)
* uploadbucket =  for uploading input csv files
* summary-bucket = for storing output csv files
* set the bucket 'uploadbucket' in the triggering part in Lambda function (workerFunction)

3- Package the Worker code using mvn-package in the root directory of the project in terminal to generate a Jar file.

4- Deploy the code by uploading generated Jar file in to the lambda function.

5- Run Client.java locally. Here I passe the input file name as a command line argument as an example 01-10-2022-store5.csv

**Note: Here we are uploading 2 input csv files (01-10-2022-store5.csv, 02-10-2022-store5). In our project we have 20 input csv files. It is difficult to upload all the files in this way. But for the consolidator part it is required to upload all input csv files and storing all output csv files in a bucket. Therefore here we are uploading all the input csv files into a bucket (uploadbucket)using AWS console manually.**

*Then we can see all the input csv files in the bucket-'uploadbucket' and the output csv files in the bucket-'summary-bucket'. In the summary-bucket there are 2 folders according to the dates(01-10-2022/,02-10-2022/).In this folders we can see the summary output csv files like store1.csv, store2.csv etc. And also these files can be  seen in local machine(in ProjectClient)*

6- Run Consolidator.java locally. Here I passe the date(01-10-2022/) as a command line argument.

*After finishing all the steps you can see the final output summary in the console and also in the local machine(in here in the Consolidator project)*

**2. Client, Worker as a Java Application  and Consolidator**

1- Create 3 Java maven projects for Client(name:Client) and Worker(name:Worker) and Consolidator(name:Consolidator).

*Here Client is more relevant to the client in Lambda application except the using 2 fifos. Consolidator is exactly same as consolidator in Lambda application.*

2- Create: EC2 instance, 2 S3 buckets(name:mybucket88888888777, name:summary-bucket) and 2 Fifo queues(name:InputFifo.fifo, name:OutputFifo.fifo).
* mybucket88888888777 =  for uploading input csv files
* summary-bucket = for storing output csv files

3- Run the Worker code as Maven Build to generate a Jar file.

4- Connect to your EC2 instance (using SSH or any other method). Then enter your credentials(vim .aws/credentials )

5- Upload the Jar file to your EC2 instance using SFTP (Use suitable method according to your OS, for me I used fileZilla on my mac).

6- Now run our worker jar file on our EC2 instance using the command java -jar Worker-0.0.1-SNAPSHOT-jar-with-dependencies.jar in the terminal.

7- Run Client.java Locally. Here I passe the input file name as a command line argument as an example 01-10-2022-store5.csv

*After finishing all these steps, we can see the input csv file(01-10-2022-store5.csv) store in the bucket (mybucket88888888777) and its output csv file(store1.csv) in the bucket(summary-bucket).And also output csv file in the local machine(in Client)*

**Note: Here we have uploading one input csv file. But we can do the same thing as previous to upload all the input csv files**

The simulation will be running (Client locally and Worker on the EC2 Server).
The Client will send a request, the Worker will satisfy it and generate a file containing the results, (which the Client will download).
At the end when the request is satisfied and the Client downloads his resultFile.txt, we empty the queues to open the door for new client(s) requests to the server.
And at this point the Client will terminate, while the Worker keep waiting for a new request (forever) until we choose to terminate it manually.

6- Finally run Consolidator.java locally. Here I passe the date(01-10-2022/) as a command line argument.

*After finishing all the steps you can see the final output summary in the console and also in the local machine(in here in the Consolidator project)*







