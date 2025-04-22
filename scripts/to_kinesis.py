import boto3
import pandas as pd
import os
from datetime import datetime
import json
import time

def load_and_sort_data(start_path, end_path):
    # Load and sort trip start data
    trip_start_files = [f for f in os.listdir(start_path) if f.endswith('.csv')]
    trip_start_data = []
    
    for file in trip_start_files:
        df = pd.read_csv(os.path.join(start_path, file))
        df['data_type'] = 'start'  # Mark as start data
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])  # Convert timestamp to datetime
        trip_start_data.append(df)
    
    trip_start_combined = pd.concat(trip_start_data, ignore_index=True)
    trip_start_sorted = trip_start_combined.sort_values(['pickup_datetime'], ascending=True)
    
    # Load and sort trip end data
    trip_end_files = [f for f in os.listdir(end_path) if f.endswith('.csv')]
    trip_end_data = []
    
    for file in trip_end_files:
        df = pd.read_csv(os.path.join(end_path, file))
        df['data_type'] = 'end'  # Mark as end data
        df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])  # Convert timestamp to datetime
        trip_end_data.append(df)
    
    trip_end_combined = pd.concat(trip_end_data, ignore_index=True)
    trip_end_sorted = trip_end_combined.sort_values(['dropoff_datetime'], ascending=True)
    
    return trip_start_sorted, trip_end_sorted

def send_trip_start_to_kinesis(data, stream_name, region='eu-west-1', delay=30):
    kinesis_client = boto3.client('kinesis', region_name=region)
    
    for _, row in data.iterrows():
        # Convert row to JSON string
        record = row.to_dict()
        record['pickup_datetime'] = str(record['pickup_datetime'])  # Convert timestamp to string
        data_json = json.dumps(record)
        
        try:
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=data_json,
                PartitionKey=str(row.name)  # Using row index as partition key
            )
            print(f"Successfully sent trip start record to Kinesis: {response['SequenceNumber']}")
        except Exception as e:
            print(f"Error sending trip start record to Kinesis: {e}")
        
        time.sleep(delay)

def send_trip_end_to_kinesis(data, stream_name, region='eu-west-1', delay=30):
    kinesis_client = boto3.client('kinesis', region_name=region)
    
    for _, row in data.iterrows():
        # Convert row to JSON string
        record = row.to_dict()
        record['dropoff_datetime'] = str(record['dropoff_datetime'])  # Convert timestamp to string
        data_json = json.dumps(record)
        
        try:
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=data_json,
                PartitionKey=str(row.name)  # Using row index as partition key
            )
            print(f"Successfully sent trip end record to Kinesis: {response['SequenceNumber']}")
        except Exception as e:
            print(f"Error sending trip end record to Kinesis: {e}")
        
        time.sleep(delay)

def main():
    # Configure these paths according to your data location
    start_data_path = '../data/Trip_Start'
    end_data_path = '../data/Trip_End'
    stream_name = 'Trips'  
    
    try:
        # Load and sort trip start and trip end data separately
        trip_start_data, trip_end_data = load_and_sort_data(start_data_path, end_data_path)
        
        # Send trip start data to Kinesis
        send_trip_start_to_kinesis(trip_start_data, stream_name)
        
        # Send trip end data to Kinesis
        send_trip_end_to_kinesis(trip_end_data, stream_name)
        
    except Exception as e:
        print(f"Error processing data: {e}")

if __name__ == "__main__":
    main()