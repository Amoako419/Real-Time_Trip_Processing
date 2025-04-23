import json
import boto3
import os
import base64
from decimal import Decimal # Important for handling numbers in DynamoDB

# Initialize DynamoDB client
dynamodb = boto3.client('dynamodb')

# --- Configuration ---
# Get DynamoDB table name from environment variables
# This is the main table where raw start/end events will be stored initially
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME')
if not DYNAMODB_TABLE_NAME:
    print("Error: DYNAMODB_TABLE_NAME environment variable not set.")
    # In a production scenario, you might want to raise an exception or handle this more robustly.
    # For this example, we'll log and exit if the variable is missing.
    exit(1) # Exit if configuration is missing

# --- Helper Function to Process a Single Kinesis Record ---
def process_kinesis_record(record):
    """
    Decodes and parses a single Kinesis record.
    Returns the parsed data (Python dict) or None if parsing fails.
    """
    try:
        # Kinesis data is base64 encoded
        # The actual data is under the 'data' key within the 'kinesis' key
        encoded_data = record['kinesis']['data']
        decoded_data = base64.b64decode(encoded_data).decode('utf-8')
        # Assuming the data is a JSON string
        parsed_data = json.loads(decoded_data)

        # --- IMPORTANT: Adjust parsing based on your actual Kinesis data structure ---
        # This is a placeholder. You need to extract the actual data you want to store.
        # Example: If your Kinesis record is {'trip_id': 'abc', 'event_type': 'start', 'timestamp': '...', ...}
        # you would prepare an item structure suitable for DynamoDB.

        # Example: Prepare item structure for DynamoDB PutItem
        # DynamoDB requires specific types (S, N, BOOL, etc.) and expects them nested.
        # Numbers should be converted to Decimal.
        # Partition Key (PK) and Sort Key (SK) design is CRITICAL for DynamoDB performance.
        # A common pattern for related items like start/end is a Composite Primary Key:
        # PK = trip_id (String)
        # SK = event_type#timestamp (String) - or just timestamp if unique enough

        trip_id = parsed_data.get('trip_id')
        event_type = parsed_data.get('event_type') # e.g., 'trip_start', 'trip_end'
        timestamp = parsed_data.get('timestamp') # Assuming a timestamp field

        if not trip_id or not event_type or not timestamp:
            print(f"Skipping record due to missing required fields (trip_id, event_type, timestamp): {parsed_data}")
            return None # Skip records that don't have essential fields

        # --- Define your DynamoDB Item Structure ---
        # This structure MUST match your table's primary key and attribute names.
        # Using 'PK' and 'SK' as example attribute names for the primary key.
        # Adjust these to match your actual table schema.
        dynamodb_item = {
            'PK': {'S': str(trip_id)}, # Partition Key: trip_id (as String)
            # Sort Key: Combine event_type and timestamp for uniqueness and ordering
            'SK': {'S': f"{event_type}#{timestamp}"},
            'trip_id': {'S': str(trip_id)}, # Store trip_id as a separate attribute too
            'event_type': {'S': event_type},
            'timestamp': {'S': str(timestamp)}, # Store timestamp as String initially

            # Add other attributes from your parsed data, converting types as needed
            # Example for a 'fare_amount' which is a number:
            # 'fare_amount': {'N': str(parsed_data.get('fare_amount'))} # Convert number to string for 'N' type

            # Example for other string attributes:
            # 'pickup_location': {'S': parsed_data.get('pickup_location')},
            # 'dropoff_location': {'S': parsed_data.get('dropoff_location')},

            # Include the full raw data payload if useful for debugging or later processing
            'raw_data': {'S': decoded_data}
        }

        # Add specific attributes based on event type
        if event_type == 'trip_start':
             # Add start-specific fields
             pickup_location = parsed_data.get('pickup_location')
             if pickup_location:
                 dynamodb_item['pickup_location'] = {'S': str(pickup_location)}
             # Add other start fields...
        elif event_type == 'trip_end':
             # Add end-specific fields
             fare_amount = parsed_data.get('fare_amount')
             if fare_amount is not None: # Check if fare_amount exists
                 # Convert number to Decimal for DynamoDB 'N' type
                 try:
                     dynamodb_item['fare_amount'] = {'N': str(Decimal(str(fare_amount)))}
                 except Exception as num_e:
                     print(f"Warning: Could not convert fare_amount '{fare_amount}' to Decimal: {num_e}")
                     # Decide how to handle non-numeric fare_amount - skip or store as string?
             dropoff_location = parsed_data.get('dropoff_location')
             if dropoff_location:
                 dynamodb_item['dropoff_location'] = {'S': str(dropoff_location)}
             # Add other end fields...

        return dynamodb_item # Return the formatted DynamoDB item

    except (json.JSONDecodeError, base64.binascii.Error, KeyError) as e:
        print(f"Error decoding or parsing Kinesis record: {e}")
        # Log the error and return None to indicate failure for this record
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
    Parses records and writes them to DynamoDB.
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

