# **Trip Data Processing and Analytics Pipeline**

### **Project Description**
This project implements a serverless data processing pipeline on AWS to ingest, store, process, and analyze trip start and trip end events. The system is designed to handle streaming data from Amazon Kinesis, store and update trip records in a single DynamoDB table, trigger a transformation process when complete trip data is available, and aggregate results for analytics in Amazon S3 using AWS Glue.

The pipeline ensures real-time data ingestion, efficient storage and processing of trip events, and the generation of key performance indicators (KPIs) for analytics. It leverages AWS services such as Kinesis, Lambda, DynamoDB, and AWS Glue to build a scalable, reliable, and cost-effective solution.

---

### **Architecture Overview**

#### **High-Level Architecture Diagram**
![High-Level Architecture Diagram](#)

**Core Flow:**
1. **Data Ingestion:** Trip start and trip end events are published to Amazon Kinesis Data Streams.
2. **Initial Processing:** A Lambda function reads batches of records from Kinesis, parses each event, and writes raw events into a DynamoDB table.
3. **Event Matching and Update:** Another Lambda function triggered by DynamoDB Streams matches start and end events for the same trip, updates the corresponding DynamoDB record, and marks the trip as "completed."
4. **Aggregation and Analytics:** An AWS Glue job periodically reads completed trip records from DynamoDB, calculates daily KPIs, and writes the aggregated results to an S3 bucket.

#### **Detailed Data Flow**
![Detailed Data Flow](#)

1. **Trip Events Published to Kinesis:**  
   - Trip start and trip end events are sent as JSON records to the Kinesis Data Stream.
   - Each record includes `trip_id`, `event_type` (`trip_start` or `trip_end`), and a timestamp.

2. **Lambda Function 1 (Kinesis Reader):**  
   - Triggered by batches of records from Kinesis.  
   - Parses each event and writes it as a new item into the DynamoDB table.  
   - Example DynamoDB Item:  
     ```json
     {
       "PK": "TRIP#<trip_id>",
       "SK": "<timestamp>",
       "event_type": "<trip_start/trip_end>",
       "fare": <value>,
       "status": "in_progress"
     }
     ```

3. **DynamoDB Streams:**  
   - Captures the insertion of new items into the DynamoDB table.  
   - Triggers Lambda Function 2 for further processing.

4. **Lambda Function 2 (DynamoDB Matcher/Updater):**  
   - Triggered by batches of records from the DynamoDB Stream.  
   - For each new item (start or end event), queries the DynamoDB table to find the corresponding counterpart event for the same `trip_id`.  
   - If both start and end events are found, updates one of the existing items (e.g., the start event) with merged data and marks the trip as "completed." Optionally, deletes the other item.

5. **AWS Glue Job:**  
   - Triggered manually or on a schedule.  
   - Reads "completed" trip records from DynamoDB.  
   - Calculates daily KPIs (total fare, trip count, average fare, min/max fare) using pandas.  
   - Writes the calculated KPIs to a CSV file in the designated S3 bucket.

---

### **AWS Services Used**

1. **Amazon Kinesis Data Streams:**  
   - Real-time ingestion of trip start and trip end events.  
   - Handles high throughput and ensures no data loss.

2. **AWS Lambda:**  
   - **Lambda Function 1 (Kinesis Reader):**  
     - Reads from Kinesis and loads raw events into DynamoDB.  
   - **Lambda Function 2 (DynamoDB Matcher/Updater):**  
     - Triggered by DynamoDB Streams, matches start/end events, and updates the single trip record in DynamoDB.

3. **Amazon DynamoDB:**  
   - Single table used to store individual raw events and updated, complete trip records.  
   - DynamoDB Streams enabled to trigger processing.  
   - Schema example:  
     ```plaintext
     PK: TRIP#<trip_id>
     SK: <timestamp>
     Attributes: event_type, fare, status
     ```

4. **AWS Glue:**  
   - Python Shell job reads processed trip data from DynamoDB, calculates daily KPIs, and writes the results to S3.

5. **Amazon S3:**  
   - Stores the final aggregated KPI results as a CSV file.  
   - Output path example: `kpi_results/daily_trip_kpis.csv`.

---

### **Data Flow**

1. **Ingestion:**  
   - Trip start and trip end events are published to the Kinesis Data Stream.

2. **Initial Storage:**  
   - Lambda Function 1 reads batches of records from Kinesis, parses each event, and writes it as a new item into the DynamoDB table.

3. **Processing and Update:**  
   - DynamoDB Streams capture the insertion of new items.  
   - Lambda Function 2 is triggered by the stream.  
   - For each new item, Lambda Function 2 queries DynamoDB to find the corresponding counterpart event for the same `trip_id`.  
   - If both start and end events are found, Lambda Function 2 updates one of the existing items with merged data and marks the trip as "completed."

4. **Aggregation and Analytics:**  
   - The AWS Glue job is triggered (manually or on a schedule).  
   - It reads "completed" trip records from DynamoDB.  
   - Uses pandas to calculate daily KPIs (total fare, trip count, average fare, min/max fare).  
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
     - Partition Key (`PK`): String (e.g., `TRIP#<trip_id>`).
     - Sort Key (`SK`): String (e.g., `<timestamp>`).
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
   - Paste the code from `lambda_kinesis_reader.py` (provided separately).
   - Configure an environment variable `DYNAMODB_TABLE_NAME` with the name of your DynamoDB table.
   - Configure a Kinesis trigger, selecting your `TripEventsStream`.
   - Assign an IAM role with permissions to:
     - Read from Kinesis.
     - Write to DynamoDB (`dynamodb:BatchWriteItem`).

5. **Deploy Lambda Function 2 (DynamoDB Matcher/Updater):**
   - Create a new Lambda function with Python 3.x runtime.
   - Paste the code from `lambda_dynamodb_matcher_updater.py` (provided separately).
   - Configure an environment variable `DYNAMODB_TABLE_NAME` with the name of your DynamoDB table.
   - Configure a DynamoDB trigger, selecting your `TripEventsTable` and its stream.
   - Assign an IAM role with permissions to:
     - Read from the DynamoDB Stream (`dynamodb:GetRecords`, `dynamodb:GetShardIterator`, etc.).
     - Perform operations on DynamoDB (`dynamodb:GetItem`, `dynamodb:UpdateItem`, `dynamodb:DeleteItem`).

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
   - Ensure each record includes at least `trip_id`, `event_type` (`trip_start` or `trip_end`), and a timestamp.

2. **Monitor Lambda Functions:**  
   - Check the CloudWatch Logs for Lambda Functions 1 and 2 to observe the ingestion, matching, and updating process.

3. **Verify DynamoDB:**  
   - Inspect your DynamoDB table to see raw items initially added by Lambda 1 and then updated/merged by Lambda 2.  
   - You should see completed trips represented by single items with a `status: "completed"` attribute.

4. **Run Glue Job:**  
   - Manually run the AWS Glue job from the Glue console.

5. **Check S3 Output:**  
   - After the Glue job completes successfully, check your specified S3 bucket and path for the `daily_trip_kpis.csv` file containing the aggregated results.

---

### **KPI Calculation**
The Glue script calculates the following daily KPIs based on the "completed" trip records in DynamoDB:

- **Total fare per day**
- **Count of trips per day**
- **Average fare per day**
- **Maximum fare per day**
- **Minimum fare per day**

---

### **Future Improvements**

1. **Implement a Dead Letter Queue (DLQ):**  
   - Add DLQs for Lambda functions to handle processing failures gracefully.

2. **Add Robust Error Handling and Logging:**  
   - Enhance error handling and logging in Lambda functions and the Glue job.

3. **Handle Partial Trips:**  
   - Implement a mechanism to handle partial trips that might never receive their counterpart event (e.g., scheduled cleanup or alerting process).

4. **Optimize DynamoDB Schema and Lambda 2 Lookup:**  
   - Optimize the DynamoDB schema and Lambda 2 lookup for very high throughput scenarios (e.g., using a different Sort Key design).

5. **Use AWS Step Functions:**  
   - Explore using AWS Step Functions for orchestrating the matching logic in Lambda 2 for more complex state management or handling delays between start/end events.

6. **Scale with Glue PySpark:**  
   - Consider using Glue PySpark for aggregation if the data volume in DynamoDB becomes very large, as it scales better than Python Shell for data processing.

7. **Automated Deployment:**  
   - Use AWS CloudFormation or AWS CDK for automated deployment.

---

### **Conclusion**
This comprehensive serverless pipeline efficiently processes streaming trip data, ensures data integrity through DynamoDB, and provides valuable insights through daily KPIs. By leveraging AWS services like Kinesis, Lambda, DynamoDB, and AWS Glue, the solution is scalable, cost-effective, and highly reliable.

