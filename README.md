# **Trip Data Processing and Analytics Pipeline**  

### **Project Description**  
This project implements a serverless data processing pipeline on AWS to ingest, store, process, and analyze trip start and trip end events. The system is designed to handle streaming data from Amazon Kinesis, store raw and processed trip records in a single DynamoDB table using a refined schema, trigger a transformation process when complete trip data is available, and aggregate results for analytics in Amazon S3 using AWS Glue.  

The pipeline ensures real-time data ingestion, efficient storage and processing of trip events, and the generation of key performance indicators (KPIs) for analytics. It leverages AWS services such as Kinesis, Lambda, DynamoDB, and AWS Glue to build a scalable, reliable, and cost-effective solution.  

---

### **Architecture Overview**  

#### **High-Level Architecture Diagram**  
![High-Level Architecture Diagram](#)  

**Core Flow:**  
1. **Data Ingestion:** Trip start and trip end events are published to Amazon Kinesis Data Streams.  
2. **Initial Loading:** A Lambda function reads batches of records from Kinesis, parses each event, and loads raw events into DynamoDB as `RAW#` items.  
3. **Event Matching and Completion:** Another Lambda function triggered by DynamoDB Streams identifies matching `RAW#` start and end events for the same trip ID, merges their data, and creates a new `COMPLETED#` item in the same DynamoDB table.  
4. **Aggregation and Analytics:** An AWS Glue job periodically reads `COMPLETED#` items from DynamoDB, calculates daily KPIs, and writes the aggregated results to an S3 bucket.  

---

#### **Detailed Data Flow**  
![Detailed Data Flow](#)  

1. **Ingestion:**  
   - Trip start and trip end events are sent as JSON records to the Kinesis Data Stream.  
   - Each record includes `trip_id`, `data_type` (`trip_start` or `trip_end`), and a timestamp (`pickup_datetime` or `dropoff_datetime`).  

2. **Raw Data Loading:**  
   - Lambda Function 1 (Kinesis Reader) is triggered by batches of records from Kinesis.  
   - Parses each event, validates/cleans numeric data (e.g., handles `NaN`), and writes it as a new item into the DynamoDB table.  
   - The item has:  
     - **Primary Key (PK):** `trip_id` (String).  
     - **Sort Key (SK):** `RAW#{data_type}#{timestamp}` (String).  
     - **Attributes:** `data_type`, `pickup_datetime`, `dropoff_datetime`, `fare`, etc.  
   - Marked with status: `"raw"`.  

3. **Event Matching:**  
   - DynamoDB Streams capture the insertion of new `RAW#` items.  
   - Lambda Function 2 (Matcher & Creator) is triggered by batches of records from the DynamoDB Stream.  
   - For each new `RAW#` item, queries the DynamoDB table using the `trip_id` (PK) and an `SK` prefix (`RAW#{counterpart_data_type}#`) to find the corresponding raw counterpart event.  

4. **Completion Logic:**  
   - If both `RAW#trip_start#` and `RAW#trip_end#` items are found for the same `trip_id`:  
     - Merges the relevant data from the start and end events.  
     - Creates a new item representing the completed trip.  
     - Writes this new item into the same DynamoDB table with the same `PK = trip_id` but a different `SK` prefix (e.g., `COMPLETED#{dropoff_datetime}`).  
     - Marked with status: `"completed"`.  
   - (Optional) Updates the status of the original `RAW#` items to `"processed_by_matcher"`, indicating they’ve been used to create a completed record.  

5. **Incomplete Records:**  
   - If the counterpart `RAW#` item is not found, the new raw item remains in the table with status: `"raw"`, waiting for its counterpart to arrive later.  

6. **Aggregation and Analytics:**  
   - The AWS Glue job is triggered manually or on a schedule.  
   - Reads data from the DynamoDB table.  
   - Filters items to process only those with an `SK` starting with `COMPLETED#` (or status: `"completed"`).  
   - Calculates daily KPIs (total fare, trip count, average fare, min/max fare) using pandas.  
   - Writes the calculated KPIs to a CSV file in the designated S3 bucket.  

---

### **AWS Services Used**  

1. **Amazon Kinesis Data Streams:**  
   - Real-time ingestion of trip start and trip end events.  
   - Handles high throughput and ensures no data loss.  

2. **AWS Lambda:**  
   - **Lambda Function 1 (Kinesis Reader & Raw Data Loader):**  
     - Reads from Kinesis, parses raw events, validates/cleans numeric data, and loads raw events into DynamoDB.  
   - **Lambda Function 2 (Matcher & Completed Trip Creator):**  
     - Triggered by DynamoDB Streams, finds matching `RAW#` start and end events, merges their data, and creates a new `COMPLETED#` item in the same DynamoDB table.  

3. **Amazon DynamoDB:**  
   - Single table used to store both raw individual trip start/end events (`RAW#` items) and processed, complete trip records (`COMPLETED#` items).  
   - DynamoDB Streams enabled to trigger processing.  
   - Schema example:  
     ```plaintext
     PK: trip_id (String)  
     SK: RAW#{data_type}#{timestamp} or COMPLETED#{dropoff_datetime}  
     Attributes: pickup_datetime, dropoff_datetime, fare, status  
     ```  

4. **AWS Glue:**  
   - Python Shell job reads processed trip data (`COMPLETED#` items) from DynamoDB, calculates daily KPIs, and writes the results to S3.  

5. **Amazon S3:**  
   - Stores the final aggregated KPI results as a CSV file.  
   - Output path example: `kpi_results/daily_trip_kpis.csv`.  

---

### **Data Flow**  

1. **Ingestion:**  
   - Trip start and trip end events are published to the Kinesis Data Stream.  

2. **Raw Data Loading:**  
   - Lambda Function 1 reads batches of records from Kinesis, parses each event, validates/cleans numeric data, and writes it as a new item into the DynamoDB table.  

3. **Event Matching and Completion:**  
   - DynamoDB Streams capture the insertion of new `RAW#` items.  
   - Lambda Function 2 is triggered by the stream.  
   - For each new `RAW#` item, queries the DynamoDB table to find the corresponding counterpart event for the same `trip_id`.  
   - If both `RAW#trip_start#` and `RAW#trip_end#` items are found, merges the data and creates a new `COMPLETED#` item.  

4. **Aggregation and Analytics:**  
   - The AWS Glue job is triggered manually or on a schedule.  
   - Reads `COMPLETED#` items from DynamoDB.  
   - Calculates daily KPIs (total fare, trip count, average fare, min/max fare) using pandas.  
   - Writes the calculated KPIs to a CSV file in the designated S3 bucket.  

---

### **Setup and Deployment**  

#### **Prerequisites**  
- AWS Account with appropriate permissions.  
- AWS CLI configured with credentials.  
- Basic understanding of AWS Kinesis, Lambda, DynamoDB, Glue, and S3.  

#### **Steps**  

1. **Create DynamoDB Table:**  
   - Create a DynamoDB table named `TripEventsTable`.  
   - Define the Primary Key:  
     - **Partition Key (PK):** `trip_id` (String).  
     - **Sort Key (SK):** `RAW#{data_type}#{timestamp}` or `COMPLETED#{dropoff_datetime}` (String).  
   - Enable DynamoDB Streams with "New Image" view type.  
   - Note the table name.  

2. **Create S3 Bucket:**  
   - Create an S3 bucket for storing the final KPI output (e.g., `your-trip-analytics-bucket`).  
   - Note the bucket name and desired output path (e.g., `kpi_results/daily_trip_kpis.csv`).  

3. **Create Kinesis Data Stream:**  
   - Create a Kinesis Data Stream named `TripEventsStream`.  
   - Note the stream name.  

4. **Deploy Lambda Function 1 (Kinesis Reader):**  
   - Create a new Lambda function with Python 3.x runtime.  
   - Paste the code from `lambda_kinesis_reader.py` (referencing the fixed version with duplicates and `NaN` handling).  
   - Configure an environment variable `DYNAMODB_TABLE_NAME` with the name of your DynamoDB table.  
   - Configure a Kinesis trigger, selecting your `TripEventsStream`.  
   - Assign an IAM role with permissions to:  
     - Read from Kinesis (`kinesis:GetRecords`).  
     - Write to DynamoDB (`dynamodb:BatchWriteItem`).  

5. **Deploy Lambda Function 2 (DynamoDB Matcher/Creator):**  
   - Create a new Lambda function with Python 3.x runtime.  
   - Paste the code from `lambda_dynamodb_matcher_creator_refined.py` (referencing the latest version).  
   - Configure an environment variable `DYNAMODB_TABLE_NAME` with the name of your DynamoDB table.  
   - Configure a DynamoDB trigger, selecting your `TripEventsTable` and its stream.  
   - Assign an IAM role with permissions to:  
     - Read from the DynamoDB Stream (`dynamodb:GetRecords`).  
     - Perform `dynamodb:Query` (for finding counterparts).  
     - Perform `dynamodb:PutItem` (for writing completed items).  
     - Perform `dynamodb:UpdateItem` (optional status update on raw items).  

6. **Deploy AWS Glue Job:**  
   - Upload the Glue Python Shell script (`glue_kpi_script.py`) to an S3 location.  
   - Create a new Glue job, selecting "Python Shell" as the job type.  
   - Specify the S3 path to your script.  
   - Configure environment variables:  
     - `DYNAMODB_TABLE_NAME`: Your DynamoDB table name.  
     - `S3_BUCKET_NAME`: Your S3 bucket name.  
     - `S3_OUTPUT_KEY`: Your desired S3 output path.  
   - Assign an IAM role with permissions to:  
     - Scan DynamoDB (`dynamodb:Scan`).  
     - Write to S3 (`s3:PutObject`).  
     - Log to CloudWatch Logs.  

---

### **Usage**  

1. **Ingest Data:**  
   - Send trip start and trip end events as JSON records to your Kinesis Data Stream.  
   - Ensure each record includes at least `trip_id`, `data_type` (`trip_start` or `trip_end`), and a timestamp (`pickup_datetime` or `dropoff_datetime`).  

2. **Monitor Lambda Functions:**  
   - Check the CloudWatch Logs for Lambda Functions 1 and 2 to observe the ingestion, matching, and creation of completed items.  

3. **Verify DynamoDB:**  
   - Inspect your DynamoDB table.  
   - You should see items with `SK` starting with `RAW#` (the initial events) and, for completed trips, new items with `SK` starting with `COMPLETED#`.  
   - The raw items might also have an updated status if you implemented that part.  

4. **Run Glue Job:**  
   - Manually run the AWS Glue job from the Glue console.  

5. **Check S3 Output:**  
   - After the Glue job completes successfully, check your specified S3 bucket and path for the `daily_trip_kpis.csv` file containing the aggregated results.  

---

### **KPI Calculation**  

The Glue script calculates the following daily KPIs based on the `COMPLETED#` items in DynamoDB:  

- **Total fare per day**  
- **Count of trips per day**  
- **Average fare per day**  
- **Maximum fare per day**  
- **Minimum fare per day**  

---

### **Future Improvements**  

1. **Dead Letter Queue (DLQ):**  
   - Implement a DLQ for Lambda functions to handle processing failures gracefully.  

2. **Robust Error Handling and Logging:**  
   - Enhance error handling and logging in Lambda functions and the Glue job.  

3. **Handling Partial Trips:**  
   - Implement a mechanism to handle partial trips (raw items that never get a counterpart).  

4. **Optimize DynamoDB Schema and Query:**  
   - Optimize the DynamoDB schema and Lambda 2 Query for very high throughput scenarios.  

5. **Use Glue PySpark:**  
   - Consider using Glue PySpark for aggregation if the data volume in DynamoDB becomes very large.  

6. **Automated Deployment:**  
   - Use AWS CloudFormation or AWS CDK for automated deployment.  

7. **Monitoring and Alerting:**  
   - Implement monitoring and alerting for pipeline health and data quality.  

---

### **Conclusion**  

This comprehensive serverless pipeline efficiently processes streaming trip data, ensures data integrity through DynamoDB, and provides valuable insights through daily KPIs. By leveraging AWS services like Kinesis, Lambda, DynamoDB, and AWS Glue, the solution is scalable, cost-effective, and highly reliable.  

Feel free to reach out if you have any questions or need further assistance!  

---  

