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

def send_paired_data_to_kinesis(start_data, end_data, stream_name, region='eu-west-1', delay=30):
    kinesis_client = boto3.client('kinesis', region_name=region)
    
    # Pair start and end data by trip_id
    paired_data = pd.merge(start_data, end_data, on='trip_id', suffixes=('_start', '_end'))
    
    for _, row in paired_data.iterrows():
        try:
            # Send trip start record
            start_record = {
                'trip_id': row['trip_id'],
                'pickup_datetime': str(row['pickup_datetime']),
                'data_type': 'start',
                'pickup_location_id': row['pickup_location_id'],
                'dropoff_location_id': row['dropoff_location_id'],
                'vendor_id': row['vendor_id'],
                'estimated_dropoff_datetime': str(row['estimated_dropoff_datetime']),
                'estimated_fare_amount': row['estimated_fare_amount']
            }
            start_response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(start_record),
                PartitionKey=str(row['trip_id'])
            )
            print(f"Successfully sent trip start record to Kinesis: {start_response['SequenceNumber']}")
            
            # Send trip end record
            end_record = {
                'trip_id': row['trip_id'],
                'dropoff_datetime': str(row['dropoff_datetime']),
                'data_type': 'end',
                'rate_code': row['rate_code'],
                'payment_type': row['payment_type'],
                'fare_amount': row['fare_amount'],
                'trip_distance': row['trip_distance'],
                'tip_amount': row['tip_amount'],
                'trip_type': row['trip_type'],
                'passenger_count': row['passenger_count']
            }
            end_response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(end_record),
                PartitionKey=str(row['trip_id'])
            )
            print(f"Successfully sent trip end record to Kinesis: {end_response['SequenceNumber']}")
            
        except Exception as e:
            print(f"Error sending paired records to Kinesis: {e}")
        
        time.sleep(delay)

def main():
    # Configure these paths according to your data location
    start_data_path = '../data/Trip_Start'
    end_data_path = '../data/Trip_End'
    stream_name = 'Trips'  
    
    try:
        # Load and sort trip start and trip end data separately
        trip_start_data, trip_end_data = load_and_sort_data(start_data_path, end_data_path)
        
        # Send paired trip start and trip end data to Kinesis
        send_paired_data_to_kinesis(trip_start_data, trip_end_data, stream_name)
        
    except Exception as e:
        print(f"Error processing data: {e}")

if __name__ == "__main__":
    main()