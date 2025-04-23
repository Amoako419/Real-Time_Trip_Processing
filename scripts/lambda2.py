import json
import boto3
import os
from decimal import Decimal # Important for handling numbers in DynamoDB

# Initialize DynamoDB client
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
            # Convert Decimal string back to Decimal object to preserve precision
            try:
                python_dict[key] = Decimal(value['N'])
            except Exception as e:
                 print(f"Warning: Could not convert DynamoDB Number '{value['N']}' for key '{key}' to Decimal: {e}")
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
def find_counterpart_item(trip_id, current_data_type):
    """
    Looks up the corresponding start or end item for a given trip_id
    in the same DynamoDB table using the PK and SK structure.
    """
    # Determine the data_type we are looking for based on the current item's data_type
    counterpart_data_type = 'trip_end' if current_data_type == 'trip_start' else 'trip_start'

    print(f"Looking for counterpart '{counterpart_data_type}' for trip ID '{trip_id}'")

    try:
        # Use Query because we know the Partition Key (PK = trip_id)
        # We can use KeyConditionExpression with the beginning of the Sort Key (SK)
        # to efficiently find items with the same trip_id and a specific data_type.
        # Our SK is formatted as "data_type#timestamp".
        # We query for items where PK = trip_id AND SK begins with "counterpart_data_type#"

        response = dynamodb.query(
            TableName=DYNAMODB_TABLE_NAME,
            KeyConditionExpression='PK = :pk_val AND begins_with(SK, :sk_prefix)',
            ExpressionAttributeValues={
                ':pk_val': {'S': str(trip_id)},
                ':sk_prefix': {'S': f"{counterpart_data_type}#"}
            },
            Limit=1 # We only expect one counterpart item with this data_type
        )

        items = response.get('Items')

        if items and len(items) > 0:
            print(f"Found counterpart item for trip ID '{trip_id}'.")
            # Return the first item found (should be only one with Limit=1)
            return items[0]
        else:
            print(f"No counterpart '{counterpart_data_type}' found for trip ID '{trip_id}'.")
            return None # No counterpart found

    except Exception as e:
        print(f"Error finding counterpart item for trip ID '{trip_id}': {e}")
        return None

# --- Helper Function to Update/Merge the Item ---
def update_merged_item(item_to_update_key, data_to_add):
    """
    Updates an existing item in DynamoDB by adding/updating attributes
    from the data_to_add dictionary.
    """
    if not item_to_update_key or not data_to_add:
        print("Cannot update item: missing key or data.")
        return

    # Build the UpdateExpression and AttributeValue updates
    update_expression_parts = []
    expression_attribute_values = {}
    expression_attribute_names = {} # Needed if attribute names are reserved words

    # --- Define Mapping for Attributes to Add/Update ---
    # This maps keys from the Python dict (data_to_add) to the DynamoDB attribute names
    # and specifies how to handle their types.
    # Add all the attributes from the 'end' event that you want to merge into the 'start' item.
    # Or vice versa, depending on which item you decide to update.
    # Assuming we are updating the 'start' item with 'end' data.

    attributes_to_merge = {
        'dropoff_datetime': 'dropoff_datetime', # Python key: DynamoDB attribute name
        'rate_code': 'rate_code',
        'payment_type': 'payment_type',
        'fare_amount': 'fare_amount',
        'trip_distance': 'trip_distance',
        'tip_amount': 'tip_amount',
        'trip_type': 'trip_type',
        'passenger_count': 'passenger_count',
        'status': 'status' # Adding a status attribute to mark completion
        # Add other fields from the 'end' event if needed
    }

    for key, db_attribute_name in attributes_to_merge.items():
        if key in data_to_add:
            # Add to UpdateExpression
            # Use placeholder names like #attrName if the attribute name is a reserved word
            placeholder_name = f"#{db_attribute_name}"
            update_expression_parts.append(f"{placeholder_name} = :{key}")

            # Add to ExpressionAttributeValues, converting to DynamoDB type
            value = data_to_add[key]
            if isinstance(value, str):
                expression_attribute_values[f':{key}'] = {'S': value}
            elif isinstance(value, Decimal): # Handle Decimal objects from unmarshalling
                 expression_attribute_values[f':{key}'] = {'N': str(value)}
            elif isinstance(value, (int, float)): # Handle raw numbers if they somehow appear
                 try:
                     expression_attribute_values[f':{key}'] = {'N': str(Decimal(str(value)))}
                 except Exception as num_e:
                      print(f"Warning: Could not convert value for '{key}' to Decimal during update: {num_e}")
                      continue # Skip this attribute if conversion fails
            elif isinstance(value, bool):
                 expression_attribute_values[f':{key}'] = {'BOOL': value}
            elif value is None:
                 expression_attribute_values[f':{key}'] = {'NULL': True}
            else:
                 print(f"Warning: Unhandled data type for key '{key}' during update: {type(value)}")
                 continue # Skip this attribute

            # Add to ExpressionAttributeNames if the attribute name is a reserved word
            expression_attribute_names[placeholder_name] = db_attribute_name

    # Also add/update attributes from the 'start' event that might be missing on the 'end' event
    # Example: estimated_dropoff_datetime, estimated_fare_amount, pickup_location_id, dropoff_location_id, vendor_id
    start_attributes_to_merge = {
         'pickup_datetime': 'pickup_datetime',
         'pickup_location_id': 'pickup_location_id',
         'dropoff_location_id': 'dropoff_location_id', # Note: This is also in end, decide which to keep or if they should match
         'vendor_id': 'vendor_id',
         'estimated_dropoff_datetime': 'estimated_dropoff_datetime',
         'estimated_fare_amount': 'estimated_fare_amount'
         # Add other start fields
    }

    # Assuming data_to_add here is the data from the *other* item (the one not being updated)
    # You need to pass the data from the *other* item to this function.
    # Let's assume data_to_add is the data from the item being merged IN (e.g., the END data if updating START)
    # We need access to the data from *both* items here to decide what to merge.

    # REVISED APPROACH: Pass both items (new_item and counterpart_item) to this function
    # and decide which attributes to pick/merge.

    # For simplicity in this helper, let's assume `data_to_add` is a dictionary containing
    # the combined attributes you want to set on the target item, with correct Python types.
    # The calling code in the handler will prepare this combined dictionary.

    if not update_expression_parts:
         print("No attributes specified to update for this item.")
         return # Nothing to update

    update_expression = "SET " + ", ".join(update_expression_parts)

    print(f"Updating item with key {item_to_update_key} using UpdateExpression: {update_expression}")
    # print(f"Attribute Values: {json.dumps(expression_attribute_values, cls=DecimalEncoder)}") # Use DecimalEncoder for printing Decimal
    # print(f"Attribute Names: {json.dumps(expression_attribute_names)}")

    try:
        response = dynamodb.update_item(
            TableName=DYNAMODB_TABLE_NAME,
            Key=item_to_update_key, # The primary key of the item to update
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ExpressionAttributeNames=expression_attribute_names, # Always include if using placeholder names
            ReturnValues="UPDATED_NEW" # Optional: return the updated attributes
        )
        print("UpdateItem successful.")
        # print(f"Updated attributes: {json.dumps(response.get('Attributes'), cls=DecimalEncoder)}") # Uncomment to see updated attributes

    except Exception as e:
        print(f"Error updating item with key {item_to_update_key}: {e}")
        # Log the error. Consider implementing retries or sending to a DLQ.

# Helper to handle Decimal serialization for JSON printing
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj) # Convert Decimal to string for JSON
        return super(DecimalEncoder, self).default(obj)


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
    Looks for matching start/end trip events and updates/deletes items.
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

                # Extract primary key components and data_type from the new item
                # Ensure these keys match your table's primary key attributes (PK, SK)
                # and the data_type attribute.
                pk_val = new_image_dynamodb_format.get('PK', {}).get('S')
                sk_val = new_image_dynamodb_format.get('SK', {}).get('S')
                new_item_data_type = new_item.get('data_type') # Get data_type from unmarshalled item

                if not pk_val or not sk_val or not new_item_data_type:
                     print(f"Skipping record: Missing primary key components (PK, SK) or data_type in NewImage: {new_image_dynamodb_format}")
                     continue

                # Extract trip_id from the PK
                trip_id = pk_val # Assuming PK is the trip_id

                # Construct the primary key dictionary for the new item
                new_item_key = {
                    'PK': {'S': pk_val},
                    'SK': {'S': sk_val}
                }

                # Find the counterpart item in the same table
                counterpart_item_dynamodb_format = find_counterpart_item(trip_id, new_item_data_type)

                if counterpart_item_dynamodb_format:
                    # Found the counterpart! Now, merge and update.

                    # Convert counterpart to Python dict for easier merging
                    counterpart_item = unmarshall_dynamodb_item(counterpart_item_dynamodb_format)
                    counterpart_data_type = counterpart_item.get('data_type') # Get data_type from unmarshalled item

                    # Construct the primary key dictionary for the counterpart item
                    counterpart_item_key = {
                        'PK': counterpart_item_dynamodb_format.get('PK', {}).get('S'),
                        'SK': counterpart_item_dynamodb_format.get('SK', {}).get('S')
                    }
                    # Validate counterpart key
                    if not counterpart_item_key.get('PK') or not counterpart_item_key.get('SK'):
                         print(f"Error: Found counterpart but missing primary key components (PK, SK): {counterpart_item_dynamodb_format}. Skipping merge.")
                         continue

                    # --- Determine which item to update and which to delete ---
                    # Let's decide to always update the 'trip_start' item and delete the 'trip_end' item
                    # when a match is found. This means the 'trip_start' item will become the
                    # single, complete trip record.

                    item_to_update_key = None # Primary key of the item that will become the merged record
                    item_to_delete_key = None # Primary key of the item to delete
                    data_to_add_to_updated_item = {} # Data from the other item to merge in

                    if new_item_data_type == 'trip_start' and counterpart_data_type == 'trip_end':
                        # The new item is 'start', the counterpart is 'end'. Update the 'start' item.
                        print("New item is 'start', counterpart is 'end'. Will update 'start' item.")
                        item_to_update_key = new_item_key # Update the 'start' item that just arrived
                        item_to_delete_key = counterpart_item_key # Delete the 'end' item found via lookup
                        data_to_add_to_updated_item = counterpart_item # Add fields from the 'end' item

                    elif new_item_data_type == 'trip_end' and counterpart_data_type == 'trip_start':
                         # The new item is 'end', the counterpart is 'start'. Update the 'start' item.
                         print("New item is 'end', counterpart is 'start'. Will update 'start' item.")
                         item_to_update_key = counterpart_item_key # Update the 'start' item found via lookup
                         item_to_delete_key = new_item_key # Delete the 'end' item that just arrived
                         data_to_add_to_updated_item = new_item # Add fields from the 'end' item

                    # --- Perform the Update and Delete operations ---
                    if item_to_update_key and data_to_add_to_updated_item:
                        # Add a status field to indicate the item is now complete
                        data_to_add_to_updated_item['status'] = 'completed'
                        # Call the update helper with the key of the item to update and the data to merge
                        update_merged_item(item_to_update_key, data_to_add_to_updated_item)

                    if item_to_delete_key:
                        # Optional: Delete the other item (the one that wasn't updated)
                        print("Optional: Deleting the counterpart item.")
                        delete_item(item_to_delete_key)


                else:
                    # No counterpart found in the table yet for this trip_id and data_type.
                    # The item remains in the table as a partial record,
                    # waiting for its counterpart to arrive later, which will
                    # trigger this Lambda again via the stream.
                    print(f"No counterpart found for trip ID '{trip_id}' with data_type '{new_item_data_type}'. Item remains as partial.")


            except Exception as e:
                print(f"An unexpected error occurred while processing DynamoDB Stream record: {e}")
                # Log the error. Depending on your Kinesis Lambda configuration,
                # this might cause the entire batch to be retried.
                # You might want more granular error handling per record.

        elif record['eventName'] == 'MODIFY' or record['eventName'] == 'REMOVE':
            # Handle other event types if necessary (e.g., logging updates/deletions)
            # We only care about INSERTs for the initial matching logic based on new items.
            pass # Skip MODIFY and REMOVE events for this logic

    # DynamoDB Stream Lambda functions typically return a response indicating success/failure
    # of processing the batch. Returning an empty success response is common.
    return {
        'statusCode': 200,
        'body': json.dumps('DynamoDB Stream batch processing complete!')
    }

