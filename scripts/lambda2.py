import json
import boto3
import os
from decimal import Decimal # Important for handling numbers in DynamoDB

# Initialize DynamoDB client
# We need the client to perform GetItem, UpdateItem, and DeleteItem operations
dynamodb = boto3.client('dynamodb')

# --- Configuration ---
# Get DynamoDB table name from environment variables
# This is the same table that Lambda 1 writes to and that has the stream enabled
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME')
if not DYNAMODB_TABLE_NAME:
    print("Error: DYNAMODB_TABLE_NAME environment variable not set.")
    exit(1)

# --- Helper Function to Convert DynamoDB Item Format to Python Dict ---
def unmarshall_dynamodb_item(dynamodb_item):
    """
    Converts a DynamoDB item (with type descriptors like {'S': 'value'})
    into a standard Python dictionary. Handles basic types.
    """
    if not dynamodb_item:
        return None

    python_dict = {}
    for key, value in dynamodb_item.items():
        if 'S' in value:
            python_dict[key] = value['S']
        elif 'N' in value:
            # Convert Decimal string back to float or int if appropriate,
            # or keep as Decimal if precision is needed.
            # For simplicity, converting to float here.
            try:
                python_dict[key] = float(value['N'])
            except ValueError:
                 python_dict[key] = value['N'] # Keep as string if conversion fails
        elif 'BOOL' in value:
            python_dict[key] = value['BOOL']
        elif 'NULL' in value:
            python_dict[key] = None
        # Add handling for other types (L, M, B, SS, NS, BS) as needed
        else:
            print(f"Warning: Unhandled DynamoDB data type for key '{key}': {value}")
            python_dict[key] = value # Store as is if type is unknown

    return python_dict

# --- Helper Function to Find the Counterpart Item ---
def find_counterpart_item(trip_id, current_event_type):
    """
    Looks up the corresponding start or end item for a given trip_id
    in the same DynamoDB table.
    """
    # Determine the event type we are looking for
    counterpart_event_type = 'trip_end' if current_event_type == 'trip_start' else 'trip_start'

    print(f"Looking for counterpart '{counterpart_event_type}' for trip ID '{trip_id}'")

    try:
        # Use Query because we know the Partition Key (trip_id)
        # We can filter by event_type using a FilterExpression
        # Note: FilterExpression is applied *after* scanning items with the same PK,
        # so it doesn't reduce read capacity units, but it filters results before returning.
        # For better efficiency with SK, you might design SK to allow direct retrieval
        # of both start and end items (e.g., SK could be 'START' and 'END').
        # Using FilterExpression for simplicity here based on the previous SK design.

        response = dynamodb.query(
            TableName=DYNAMODB_TABLE_NAME,
            KeyConditionExpression='PK = :pk_val',
            FilterExpression='event_type = :event_type_val',
            ExpressionAttributeValues={
                ':pk_val': {'S': str(trip_id)},
                ':event_type_val': {'S': counterpart_event_type}
            },
            Limit=1 # We only expect one counterpart item
        )

        items = response.get('Items')

        if items and len(items) > 0:
            print(f"Found counterpart item for trip ID '{trip_id}'.")
            # Return the first item found (should be only one with Limit=1)
            return items[0]
        else:
            print(f"No counterpart '{counterpart_event_type}' found for trip ID '{trip_id}'.")
            return None # No counterpart found

    except Exception as e:
        print(f"Error finding counterpart item for trip ID '{trip_id}': {e}")
        # Log the error and return None
        return None

# --- Helper Function to Update/Merge the Item ---
def update_merged_item(existing_item_key, data_to_add):
    """
    Updates an existing item in DynamoDB by adding/updating attributes
    from the data_to_add dictionary.
    """
    if not existing_item_key or not data_to_add:
        print("Cannot update item: missing key or data.")
        return

    # Build the UpdateExpression and AttributeValue updates
    update_expression_parts = []
    expression_attribute_values = {}
    expression_attribute_names = {} # Needed if attribute names are reserved words

    # Example: Adding end-trip related attributes to a start-trip item
    # Assuming data_to_add contains fields like 'end_timestamp', 'fare_amount', 'dropoff_location'
    # You need to map the Python dict keys in data_to_add to DynamoDB attribute names and types.

    # Example mapping:
    attribute_map = {
        'end_timestamp': 'end_timestamp', # Python key: DynamoDB attribute name
        'fare_amount': 'fare_amount',
        'dropoff_location': 'dropoff_location',
        'status': 'status' # Adding a status attribute
        # Add mappings for other fields you want to merge
    }

    for key, db_attribute_name in attribute_map.items():
        if key in data_to_add:
            # Add to UpdateExpression
            update_expression_parts.append(f"#{db_attribute_name} = :{key}")

            # Add to ExpressionAttributeValues, converting to DynamoDB type
            value = data_to_add[key]
            if isinstance(value, str):
                expression_attribute_values[f':{key}'] = {'S': value}
            elif isinstance(value, (int, float, Decimal)):
                 # Ensure numbers are stored as Decimal for 'N' type
                 try:
                     expression_attribute_values[f':{key}'] = {'N': str(Decimal(str(value)))}
                 except Exception as num_e:
                      print(f"Warning: Could not convert value for '{key}' to Decimal: {num_e}")
                      # Decide how to handle - skip or store as string? Skipping for now.
                      continue # Skip this attribute if conversion fails
            elif isinstance(value, bool):
                 expression_attribute_values[f':{key}'] = {'BOOL': value}
            elif value is None:
                 expression_attribute_values[f':{key}'] = {'NULL': True}
            # Add handling for other types (L, M, B, SS, NS, BS) as needed
            else:
                 print(f"Warning: Unhandled data type for key '{key}' during update: {type(value)}")
                 # Decide how to handle - skip or store as string? Skipping for now.
                 continue # Skip this attribute

            # Add to ExpressionAttributeNames if the attribute name is a reserved word
            # For common names like 'status', 'timestamp', 'fare_amount', it's good practice
            expression_attribute_names[f'#{db_attribute_name}'] = db_attribute_name


    if not update_expression_parts:
        print("No attributes to update for this item.")
        return # Nothing to update

    update_expression = "SET " + ", ".join(update_expression_parts)

    print(f"Updating item with key {existing_item_key} using UpdateExpression: {update_expression}")
    print(f"Attribute Values: {expression_attribute_values}")
    print(f"Attribute Names: {expression_attribute_names}")


    try:
        response = dynamodb.update_item(
            TableName=DYNAMODB_TABLE_NAME,
            Key=existing_item_key, # The primary key of the item to update
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            # Only include ExpressionAttributeNames if needed (if any attribute name is a reserved word)
            ExpressionAttributeNames=expression_attribute_names if expression_attribute_names else None,
            ReturnValues="UPDATED_NEW" # Optional: return the updated attributes
        )
        print("UpdateItem successful.")
        # print(f"Updated attributes: {json.dumps(response.get('Attributes'))}") # Uncomment to see updated attributes

    except Exception as e:
        print(f"Error updating item with key {existing_item_key}: {e}")
        # Log the error. Consider implementing retries or sending to a DLQ.


# --- Helper Function to Delete an Item ---
def delete_item(item_key):
    """
    Deletes an item from the DynamoDB table.
    """
    if not item_key:
        print("Cannot delete item: missing key.")
        return

    print(f"Deleting item with key: {item_key}")

    try:
        dynamodb.delete_item(
            TableName=DYNAMODB_TABLE_NAME,
            Key=item_key # The primary key of the item to delete
        )
        print("DeleteItem successful.")
    except Exception as e:
        print(f"Error deleting item with key {item_key}: {e}")
        # Log the error. Consider implementing retries.


# --- Main Lambda Handler ---

def lambda_handler(event, context):
    """
    AWS Lambda handler for processing DynamoDB Stream events.
    Looks for matching start/end trip events and updates the item.
    """
    print(f"Received DynamoDB Stream event with {len(event['Records'])} records.")

    for record in event['Records']:
        # We are primarily interested in INSERT events for new items written by Lambda 1
        if record['eventName'] == 'INSERT':
            try:
                # The new item image is under 'NewImage'
                new_image_dynamodb_format = record.get('dynamodb', {}).get('NewImage')
                if not new_image_dynamodb_format:
                    print("Skipping record: NewImage not found.")
                    continue

                # Convert the DynamoDB item format to a standard Python dict
                new_item = unmarshall_dynamodb_item(new_image_dynamodb_format)
                print(f"Processing new item: {new_item}")

                # Extract primary key and event type from the new item
                # Ensure these keys match your table's primary key attributes
                trip_id = new_item.get('trip_id') # Assuming 'trip_id' is an attribute
                event_type = new_item.get('event_type') # Assuming 'event_type' is an attribute

                # Also need the actual primary key structure for GetItem/UpdateItem/DeleteItem
                # This MUST match your table's primary key attributes (e.g., 'PK', 'SK')
                new_item_key = {
                    'PK': new_image_dynamodb_format.get('PK'), # Get the PK value in DynamoDB format
                    'SK': new_image_dynamodb_format.get('SK')  # Get the SK value in DynamoDB format
                }
                # Validate that primary key components were found
                if not new_item_key.get('PK') or not new_item_key.get('SK'):
                     print(f"Skipping record: Primary key components (PK, SK) not found in NewImage: {new_image_dynamodb_format}")
                     continue


                if not trip_id or not event_type:
                    print(f"Skipping record: Missing trip_id or event_type in new item: {new_item}")
                    continue

                # Find the counterpart item in the same table
                counterpart_item_dynamodb_format = find_counterpart_item(trip_id, event_type)

                if counterpart_item_dynamodb_format:
                    # Found the counterpart! Now, merge and update.

                    # Convert counterpart to Python dict for easier merging
                    counterpart_item = unmarshall_dynamodb_item(counterpart_item_dynamodb_format)

                    # --- Determine which item to update and which to delete ---
                    # You need a consistent rule here. Example: Always update the 'trip_start' item
                    # and delete the 'trip_end' item. Or update the item that arrived second.
                    # Let's assume we update the 'trip_start' item if the new item is 'trip_end',
                    # and vice-versa. The item that exists *before* this stream event is the one
                    # we found via `find_counterpart_item`. The item *from* the stream event
                    # is the `new_item`.

                    item_to_update_key = None # Primary key of the item that will become the merged record
                    item_to_delete_key = None # Primary key of the item to delete

                    if event_type == 'trip_end':
                        # The new item is 'end'. The counterpart is 'start'. Update the 'start' item.
                        print("New item is 'end', counterpart is 'start'. Will update 'start' item.")
                        item_to_update_key = {
                             'PK': counterpart_item_dynamodb_format.get('PK'),
                             'SK': counterpart_item_dynamodb_format.get('SK')
                        }
                        item_to_delete_key = new_item_key # Delete the 'end' item that just arrived

                        # Data to add to the 'start' item comes from the 'end' item
                        data_to_add = new_item # Add fields from the 'end' item

                    elif event_type == 'trip_start':
                         # The new item is 'start'. The counterpart is 'end'. Update the 'end' item.
                         # This might be less common as 'end' often has the final fare.
                         # Let's stick to updating 'start' for consistency in this example.
                         # If the new item is 'start' and we found the 'end' counterpart,
                         # it means the 'end' arrived first. We should still update the 'start' item.
                         print("New item is 'start', counterpart is 'end'. Will update 'start' item.")
                         item_to_update_key = new_item_key # Update the 'start' item that just arrived
                         item_to_delete_key = {
                             'PK': counterpart_item_dynamodb_format.get('PK'),
                             'SK': counterpart_item_dynamodb_format.get('SK')
                         } # Delete the 'end' item found via lookup

                         # Data to add to the 'start' item comes from the 'end' item
                         data_to_add = counterpart_item # Add fields from the 'end' item

                    # --- Perform the Update and Delete operations ---
                    if item_to_update_key and data_to_add:
                        # Add a status field to indicate the item is now complete
                        data_to_add['status'] = 'completed'
                        update_merged_item(item_to_update_key, data_to_add)

                    if item_to_delete_key:
                        # Optional: Delete the other item
                        print("Optional: Deleting the counterpart item.")
                        delete_item(item_to_delete_key)


                else:
                    # No counterpart found in the table yet.
                    # The item remains in the table as a partial record,
                    # waiting for its counterpart to arrive later, which will
                    # trigger this Lambda again via the stream.
                    print(f"No counterpart found for trip ID '{trip_id}'. Item remains as partial.")


            except Exception as e:
                print(f"An unexpected error occurred while processing DynamoDB Stream record: {e}")
                # Log the error. Depending on your Kinesis Lambda configuration,
                # this might cause the entire batch to be retried.
                # You might want more granular error handling per record.

        elif record['eventName'] == 'MODIFY' or record['eventName'] == 'REMOVE':
            # Handle other event types if necessary (e.g., logging updates/deletions)
            print(f"Skipping record with eventName: {record['eventName']}")
            pass # We only care about INSERTs for the initial matching logic

    # DynamoDB Stream Lambda functions typically return a response indicating success/failure
    # of processing the batch. Returning an empty success response is common.
    return {
        'statusCode': 200,
        'body': json.dumps('DynamoDB Stream batch processing complete!')
    }

