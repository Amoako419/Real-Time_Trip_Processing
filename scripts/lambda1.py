import json
import boto3
import os
import base64
from decimal import Decimal # Important for handling numbers in DynamoDB
from datetime import datetime # To potentially parse timestamps if needed for sorting/logic

# Initialize DynamoDB client
dynamodb = boto3.client('dynamodb')

# --- Configuration ---
# Get DynamoDB table name from environment variables
# This is the main table where raw start/end events will be stored initially
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME')
if not DYNAMODB_TABLE_NAME:
    print("Error: DYNAMODB_TABLE_NAME environment variable not set.")
    exit(1) # Exit if configuration is missing

# --- Helper Function to Process a Single Kinesis Record ---
def process_kinesis_record(record):
    """
    Decodes and parses a single Kinesis record based on the provided structure.
    Returns the parsed data (Python dict) formatted for DynamoDB or None if parsing fails.
    """
    try:
        # Kinesis data is base64 encoded
        encoded_data = record['kinesis']['data']
        decoded_data = base64.b64decode(encoded_data).decode('utf-8')
        # Assuming the data is a JSON string matching the provided examples
        parsed_data = json.loads(decoded_data)

        # --- Extract key fields based on the provided structure ---
        trip_id = parsed_data.get('trip_id')
        data_type = parsed_data.get('data_type') # 'trip_start' or 'trip_end'

        # Determine the primary timestamp field based on data_type
        timestamp_str = None
        if data_type == 'trip_start':
            timestamp_str = parsed_data.get('pickup_datetime')
        elif data_type == 'trip_end':
            timestamp_str = parsed_data.get('dropoff_datetime')

        if not trip_id or not data_type or not timestamp_str:
            print(f"Skipping record due to missing required fields (trip_id, data_type, timestamp): {parsed_data}")
            return None # Skip records that don't have essential fields

        # --- Define your DynamoDB Item Structure ---
        # Using 'PK' and 'SK' as example attribute names for the primary key.
        # Adjust these to match your actual table schema.
        # PK = trip_id (String)
        # SK = data_type#timestamp (String) - Using data_type to differentiate start/end with same trip_id/timestamp

        dynamodb_item = {
            'PK': {'S': str(trip_id)}, # Partition Key: trip_id (as String)
            # Sort Key: Combine data_type and timestamp for uniqueness and ordering
            'SK': {'S': f"{data_type}#{timestamp_str}"},
            'trip_id': {'S': str(trip_id)}, # Store trip_id as a separate attribute too
            'data_type': {'S': data_type}, # Store the event type
            'timestamp': {'S': str(timestamp_str)}, # Store the primary timestamp as String

            # Store the full raw data payload if useful for debugging or later processing
            'raw_data': {'S': decoded_data}
        }

        # --- Add specific attributes based on data_type ---
        if data_type == 'trip_start':
             # Add start-specific fields from the provided example
             pickup_datetime = parsed_data.get('pickup_datetime')
             if pickup_datetime is not None:
                 dynamodb_item['pickup_datetime'] = {'S': str(pickup_datetime)}

             pickup_location_id = parsed_data.get('pickup_location_id')
             if pickup_location_id is not None:
                 # Convert number to Decimal for 'N' type
                 try:
                     dynamodb_item['pickup_location_id'] = {'N': str(Decimal(str(pickup_location_id)))}
                 except Exception as num_e:
                     print(f"Warning: Could not convert pickup_location_id '{pickup_location_id}' to Decimal: {num_e}")
                     # Decide how to handle non-numeric - skip or store as string? Skipping for now.

             dropoff_location_id = parsed_data.get('dropoff_location_id')
             if dropoff_location_id is not None:
                 try:
                     dynamodb_item['dropoff_location_id'] = {'N': str(Decimal(str(dropoff_location_id)))}
                 except Exception as num_e:
                     print(f"Warning: Could not convert dropoff_location_id '{dropoff_location_id}' to Decimal: {num_e}")

             vendor_id = parsed_data.get('vendor_id')
             if vendor_id is not None:
                 try:
                     dynamodb_item['vendor_id'] = {'N': str(Decimal(str(vendor_id)))}
                 except Exception as num_e:
                     print(f"Warning: Could not convert vendor_id '{vendor_id}' to Decimal: {num_e}")

             estimated_dropoff_datetime = parsed_data.get('estimated_dropoff_datetime')
             if estimated_dropoff_datetime is not None:
                 dynamodb_item['estimated_dropoff_datetime'] = {'S': str(estimated_dropoff_datetime)}

             estimated_fare_amount = parsed_data.get('estimated_fare_amount')
             if estimated_fare_amount is not None:
                 try:
                     dynamodb_item['estimated_fare_amount'] = {'N': str(Decimal(str(estimated_fare_amount)))}
                 except Exception as num_e:
                     print(f"Warning: Could not convert estimated_fare_amount '{estimated_fare_amount}' to Decimal: {num_e}")


        elif data_type == 'trip_end':
             # Add end-specific fields from the provided example
             dropoff_datetime = parsed_data.get('dropoff_datetime')
             if dropoff_datetime is not None:
                 dynamodb_item['dropoff_datetime'] = {'S': str(dropoff_datetime)}

             rate_code = parsed_data.get('rate_code')
             if rate_code is not None:
                 try:
                     dynamodb_item['rate_code'] = {'N': str(Decimal(str(rate_code)))}
                 except Exception as num_e:
                     print(f"Warning: Could not convert rate_code '{rate_code}' to Decimal: {num_e}")

             payment_type = parsed_data.get('payment_type')
             if payment_type is not None:
                 try:
                     dynamodb_item['payment_type'] = {'N': str(Decimal(str(payment_type)))}
                 except Exception as num_e:
                     print(f"Warning: Could not convert payment_type '{payment_type}' to Decimal: {num_e}")

             fare_amount = parsed_data.get('fare_amount')
             if fare_amount is not None:
                 try:
                     dynamodb_item['fare_amount'] = {'N': str(Decimal(str(fare_amount)))}
                 except Exception as num_e:
                     print(f"Warning: Could not convert fare_amount '{fare_amount}' to Decimal: {num_e}")

             trip_distance = parsed_data.get('trip_distance')
             if trip_distance is not None:
                 try:
                     dynamodb_item['trip_distance'] = {'N': str(Decimal(str(trip_distance)))}
                 except Exception as num_e:
                     print(f"Warning: Could not convert trip_distance '{trip_distance}' to Decimal: {num_e}")

             tip_amount = parsed_data.get('tip_amount')
             if tip_amount is not None:
                 try:
                     dynamodb_item['tip_amount'] = {'N': str(Decimal(str(tip_amount)))}
                 except Exception as num_e:
                     print(f"Warning: Could not convert tip_amount '{tip_amount}' to Decimal: {num_e}")

             trip_type = parsed_data.get('trip_type')
             if trip_type is not None:
                 try:
                     dynamodb_item['trip_type'] = {'N': str(Decimal(str(trip_type)))}
                 except Exception as num_e:
                     print(f"Warning: Could not convert trip_type '{trip_type}' to Decimal: {num_e}")

             passenger_count = parsed_data.get('passenger_count')
             if passenger_count is not None:
                 try:
                     dynamodb_item['passenger_count'] = {'N': str(Decimal(str(passenger_count)))}
                 except Exception as num_e:
                     print(f"Warning: Could not convert passenger_count '{passenger_count}' to Decimal: {num_e}")


        return dynamodb_item # Return the formatted DynamoDB item

    except (json.JSONDecodeError, base64.binascii.Error, KeyError) as e:
        print(f"Error decoding or parsing Kinesis record: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while processing Kinesis record: {e}")
        return None

# --- Helper Function to Write Items to DynamoDB in Batches ---
def batch_write_to_dynamodb(table_name, items):
    """
    Writes a list of items to DynamoDB using batch_write_item.
    Handles the 25 item limit per batch request.
    Implements basic retry for unprocessed items.
    """
    if not table_name:
        print("DynamoDB table name not configured. Cannot write items.")
        return

    if not items:
        print("No items to write to DynamoDB.")
        return

    print(f"Attempting to write {len(items)} items to {table_name} in batches.")

    unprocessed_items = list(items) # Start with all items as potentially unprocessed
    retry_count = 0
    max_retries = 3 # Define maximum retry attempts

    while unprocessed_items and retry_count < max_retries:
        request_items = {
            table_name: [
                {'PutRequest': {'Item': item}} for item in unprocessed_items[:25] # Take up to 25 items
            ]
        }
        items_in_current_batch = unprocessed_items[:25]

        try:
            response = dynamodb.batch_write_item(
                RequestItems=request_items
            )

            # Check the response for unprocessed items
            unprocessed_in_response = response.get('UnprocessedItems', {}).get(table_name, [])

            if unprocessed_in_response:
                print(f"Warning: {len(unprocessed_in_response)} unprocessed items in batch {retry_count}. Retrying...")
                # Replace unprocessed_items with the list from the response for the next retry
                unprocessed_items = [req['PutRequest']['Item'] for req in unprocessed_in_response]
                retry_count += 1
                # Add a delay before retrying in a real application (e.g., time.sleep)
            else:
                # If no unprocessed items in the response, these items were processed.
                # Remove the processed items from the unprocessed_items list.
                processed_count = len(items_in_current_batch) - len(unprocessed_in_response)
                unprocessed_items = unprocessed_items[len(items_in_current_batch):]
                print(f"Successfully processed {processed_count} items in batch {retry_count}.")
                if not unprocessed_items:
                     print("All items processed successfully.")
                     break # Exit the loop if all items are processed

        except Exception as e:
            print(f"Error during batch_write_item to {table_name} (Retry {retry_count}): {e}")
            # Log the error. Depending on your strategy, you might stop,
            # send the whole batch to a DLQ, or let the Kinesis Lambda retry mechanism handle it.
            # For now, we'll break the retry loop on unexpected errors.
            break

    if unprocessed_items:
        print(f"After {max_retries} retries, {len(unprocessed_items)} items remain unprocessed.")
        # Implement logic to handle these failures (e.g., log them prominently,
        # send to a Dead Letter Queue configured for the Lambda).

# --- Main Lambda Handler ---

def lambda_handler(event, context):
    """
    AWS Lambda handler for processing Kinesis Data Streams batches.
    Parses records based on provided structure and writes them to DynamoDB.
    """
    print(f"Received Kinesis event with {len(event['Records'])} records.")

    items_for_dynamodb = []

    # Process each record in the batch
    for record in event['Records']:
        dynamodb_item = process_kinesis_record(record)
        if dynamodb_item:
            items_for_dynamodb.append(dynamodb_item)
        # Note: process_kinesis_record handles logging for skipped/failed records

    # Write the prepared items to DynamoDB in batches
    if items_for_dynamodb:
        batch_write_to_dynamodb(DYNAMODB_TABLE_NAME, items_for_dynamodb)
    else:
        print("No valid items to write to DynamoDB from this batch.")

    # Kinesis Lambda functions typically return a response indicating success/failure
    # of processing the batch. Returning an empty success response is common,
    # but you can also return a partial batch success response if configured.
    # For simplicity here, we assume full batch success if no exceptions were raised
    # that stopped execution. UnprocessedItems handling is within batch_write_to_dynamodb.
    return {
        'statusCode': 200,
        'body': json.dumps('Kinesis batch processing and initial loading complete!')
    }

