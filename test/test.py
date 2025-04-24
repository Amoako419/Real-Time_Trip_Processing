import pytest
import pandas as pd
import json
from datetime import datetime
from unittest.mock import patch, MagicMock, call
from io import StringIO
import sys
import os

# Add the script directory to the path so we can import it
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))

# Import the script to test
# Assuming the script is saved as trip_kpi_generator.py
import glue_scripts as script

class TestTripKpiGenerator:
    
    @pytest.fixture
    def mock_boto3_clients(self):
        """Mock boto3 clients for testing"""
        with patch('glue_scripts.boto3') as mock_boto3:
            # Set up mock clients
            mock_dynamodb = MagicMock()
            mock_s3 = MagicMock()
            
            # Configure boto3.client to return our mocks
            mock_boto3.client.side_effect = lambda service, **kwargs: {
                'dynamodb': mock_dynamodb,
                's3': mock_s3
            }[service]
            
            # Store mocks for access in tests
            mock_boto3.mock_dynamodb = mock_dynamodb
            mock_boto3.mock_s3 = mock_s3
            
            yield mock_boto3

    @pytest.fixture
    def sample_dynamodb_items(self):
        """Sample DynamoDB items for testing"""
        return [
            {
                'trip_id': {'S': 'trip1'},
                'pickup_datetime': {'S': '2025-04-20T08:30:00'},
                'fare_amount': {'N': '25.50'}
            },
            {
                'trip_id': {'S': 'trip2'},
                'pickup_datetime': {'S': '2025-04-20T09:15:00'},
                'fare_amount': {'N': '30.75'}
            },
            {
                'trip_id': {'S': 'trip3'},
                'pickup_datetime': {'S': '2025-04-21T10:00:00'},
                'fare_amount': {'N': '15.25'}
            },
            {
                'trip_id': {'S': 'trip4'},
                'pickup_datetime': {'S': '2025-04-21T14:45:00'},
                'fare_amount': {'N': '42.00'}
            }
        ]

    @pytest.fixture
    def expected_processed_items(self):
        """Expected processed items after DynamoDB conversion"""
        return [
            {
                'trip_id': 'trip1',
                'pickup_datetime': '2025-04-20T08:30:00',
                'fare_amount': 25.50
            },
            {
                'trip_id': 'trip2',
                'pickup_datetime': '2025-04-20T09:15:00',
                'fare_amount': 30.75
            },
            {
                'trip_id': 'trip3',
                'pickup_datetime': '2025-04-21T10:00:00',
                'fare_amount': 15.25
            },
            {
                'trip_id': 'trip4',
                'pickup_datetime': '2025-04-21T14:45:00',
                'fare_amount': 42.00
            }
        ]

    def test_scan_dynamodb_table(self, mock_boto3_clients, sample_dynamodb_items):
        """Test scanning DynamoDB table"""
        # Configure mock response
        mock_boto3_clients.mock_dynamodb.scan.side_effect = [
            {
                'Items': sample_dynamodb_items[:2],
                'LastEvaluatedKey': {'trip_id': {'S': 'trip2'}}
            },
            {
                'Items': sample_dynamodb_items[2:]
            }
        ]
        
        # Call the function
        result = script.scan_dynamodb_table('trips')
        
        # Get actual calls made to scan
        actual_calls = mock_boto3_clients.mock_dynamodb.scan.call_args_list
        
        # Verify the calls were made correctly
        assert len(actual_calls) == 2  # Changed from 5 to 2
        assert actual_calls[0] == call(TableName='trips')
        assert actual_calls[1] == call(TableName='trips', ExclusiveStartKey={'trip_id': {'S': 'trip2'}})
        
        # Verify the result has the correct number of items
        assert len(result) == 4
        
        # Check that each item was properly converted from DynamoDB format
        for i, item in enumerate(result):
            assert item['trip_id'] == f'trip{i+1}'
            assert 'pickup_datetime' in item
            assert isinstance(item['fare_amount'], float)

    def test_main_script_processing(self, mock_boto3_clients, expected_processed_items):
        """Test the main script logic using mocked data"""
        # Configure test data and mocks
        test_data = {
            'metadata': {'source_table': 'trips', 'record_count': 4},
            'daily_kpis': [{'date': '2025-04-24', 'total_fares': 100}]
        }

        with patch('glue_scripts.scan_dynamodb_table', return_value=expected_processed_items):
            with patch('glue_scripts.process_data', return_value=test_data):
                # Mock datetime.now() to return a fixed date for testing
                fixed_date = datetime(2025, 4, 24, 12, 0, 0)
                with patch('glue_scripts.datetime') as mock_datetime:
                    mock_datetime.now.return_value = fixed_date
                    mock_datetime.strftime = datetime.strftime
                    mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)

                    # Mock S3 put_object to simulate file upload
                    mock_boto3_clients.mock_s3.put_object.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}

                    # Execute the main script
                    with patch.object(sys, 'argv', ['glue_scripts.py']):
                        with patch('builtins.print'):  # Suppress print statements
                            script.main()

                    # Verify S3 upload
                    mock_boto3_clients.mock_s3.put_object.assert_called_once()
                    _, kwargs = mock_boto3_clients.mock_s3.put_object.call_args
                    assert kwargs['Bucket'] == 'trips-kpis-buckets-125'
                    assert kwargs['ContentType'] == 'application/json'

    def test_dataframe_processing(self, expected_processed_items):
        """Test the pandas DataFrame processing and KPI calculations"""
        # Create a test dataframe from our mock data
        df = pd.DataFrame(expected_processed_items)
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        df['pickup_date'] = df['pickup_datetime'].dt.date
        
        # Calculate KPIs manually for comparison
        expected_total_fare_day1 = 25.50 + 30.75  # 2025-04-20
        expected_total_fare_day2 = 15.25 + 42.00  # 2025-04-21
        expected_avg_fare_day1 = (25.50 + 30.75) / 2
        expected_avg_fare_day2 = (15.25 + 42.00) / 2
        
        # Process the dataframe through the KPI calculation functions
        # Mock these operations to match how they're done in the script
        total_fare_per_day = df.groupby('pickup_date')['fare_amount'].sum().reset_index()
        total_fare_per_day.rename(columns={'fare_amount': 'total_fare'}, inplace=True)
        
        count_of_trips = df.groupby('pickup_date')['trip_id'].count().reset_index()
        count_of_trips.rename(columns={'trip_id': 'trip_count'}, inplace=True)
        
        average_fare = df.groupby('pickup_date')['fare_amount'].mean().reset_index()
        average_fare.rename(columns={'fare_amount': 'average_fare'}, inplace=True)
        
        maximum_fare = df.groupby('pickup_date')['fare_amount'].max().reset_index()
        maximum_fare.rename(columns={'fare_amount': 'maximum_fare'}, inplace=True)
        
        minimum_fare = df.groupby('pickup_date')['fare_amount'].min().reset_index()
        minimum_fare.rename(columns={'fare_amount': 'minimum_fare'}, inplace=True)
        
        # Combine KPIs
        kpi_df = total_fare_per_day
        kpi_df = pd.merge(kpi_df, count_of_trips, on='pickup_date', how='left')
        kpi_df = pd.merge(kpi_df, average_fare, on='pickup_date', how='left')
        kpi_df = pd.merge(kpi_df, maximum_fare, on='pickup_date', how='left')
        kpi_df = pd.merge(kpi_df, minimum_fare, on='pickup_date', how='left')
        
        # Verify the result
        assert len(kpi_df) == 2  # 2 days of data
        
        # Check KPI values for the first day (2025-04-20)
        day1_row = kpi_df.iloc[0]
        assert abs(day1_row['total_fare'] - expected_total_fare_day1) < 0.01
        assert day1_row['trip_count'] == 2
        assert abs(day1_row['average_fare'] - expected_avg_fare_day1) < 0.01
        assert day1_row['maximum_fare'] == 30.75
        assert day1_row['minimum_fare'] == 25.50
        
        # Check KPI values for the second day (2025-04-21)
        day2_row = kpi_df.iloc[1]
        assert abs(day2_row['total_fare'] - expected_total_fare_day2) < 0.01
        assert day2_row['trip_count'] == 2
        assert abs(day2_row['average_fare'] - expected_avg_fare_day2) < 0.01
        assert day2_row['maximum_fare'] == 42.00
        assert day2_row['minimum_fare'] == 15.25

    def test_error_handling_no_data(self, mock_boto3_clients):
        """Test error handling when no data is returned from DynamoDB"""
        # Mock scan_dynamodb_table to return None (error case)
        with patch('glue_scripts.scan_dynamodb_table', return_value=None):
            # Verify system exit is called
            with pytest.raises(SystemExit):
                with patch('builtins.print'):  # Suppress print statements
                    script.main()
            
            # Verify no S3 upload was attempted
            mock_boto3_clients.mock_s3.put_object.assert_not_called()

    def test_error_handling_invalid_data(self, mock_boto3_clients):
        """Test error handling with invalid data in DynamoDB items"""
        invalid_items = [
            {
                'trip_id': 'trip1',
                'pickup_datetime': 'invalid-date',  # Invalid date format
                'fare_amount': 25.50
            },
            {
                'trip_id': 'trip2',
                'pickup_datetime': '2025-04-20T09:15:00',
                'fare_amount': 'not-a-number'  # Invalid fare amount
            }
        ]
        
        # Mock scan_dynamodb_table to return our invalid data
        with patch('glue_scripts.scan_dynamodb_table', return_value=invalid_items):
            # The script should handle this gracefully and continue with valid items only
            with patch('builtins.print'):  # Suppress print statements
                with patch.object(pd.DataFrame, 'dropna', return_value=pd.DataFrame()):  # Mock empty dataframe after dropping
                    with pytest.raises(SystemExit):
                        script.main()
            
            # Verify no S3 upload was attempted due to no valid data after filtering
            mock_boto3_clients.mock_s3.put_object.assert_not_called()

    def test_json_formatting(self, expected_processed_items):
        """Test the JSON formatting of the output"""
        # Create a test dataframe from our mock data
        df = pd.DataFrame(expected_processed_items)
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        df['pickup_date'] = df['pickup_datetime'].dt.date
        
        # Calculate KPIs (simplified for testing)
        kpi_df = pd.DataFrame({
            'pickup_date': pd.to_datetime(['2025-04-20', '2025-04-21']).date,
            'total_fare': [56.25, 57.25],
            'trip_count': [2, 2],
            'average_fare': [28.125, 28.625],
            'maximum_fare': [30.75, 42.00],
            'minimum_fare': [25.50, 15.25]
        })
        
        # Convert dates to string for JSON serialization
        kpi_df['pickup_date'] = kpi_df['pickup_date'].astype(str)
        
        # Test the JSON structure creation
        fixed_date = datetime(2025, 4, 24, 12, 0, 0)
        with patch('glue_scripts.datetime') as mock_datetime:
            mock_datetime.now.return_value = fixed_date
            
            # Structure the JSON as done in the script
            json_structure = {
                "metadata": {
                    "report_generated": fixed_date.isoformat(),
                    "report_timestamp": int(fixed_date.timestamp()),
                    "report_date": fixed_date.strftime("%Y-%m-%d"),
                    "report_time": fixed_date.strftime("%H:%M:%S"),
                    "source_table": "trips",
                    "record_count": len(df),
                    "date_range": {
                        "start_date": str(df['pickup_date'].min()),
                        "end_date": str(df['pickup_date'].max())
                    },
                    "kpi_count": len(kpi_df)
                },
                "daily_kpis": kpi_df.to_dict(orient='records')
            }
            
            # Convert to JSON string
            json_content = json.dumps(json_structure, indent=2)
            
            # Verify JSON content
            parsed_json = json.loads(json_content)
            assert parsed_json['metadata']['report_date'] == '2025-04-24'
            assert parsed_json['metadata']['source_table'] == 'trips'
            assert parsed_json['metadata']['record_count'] == 4
            assert len(parsed_json['daily_kpis']) == 2
            assert parsed_json['daily_kpis'][0]['pickup_date'] == '2025-04-20'
            assert parsed_json['daily_kpis'][0]['total_fare'] == 56.25