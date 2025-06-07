import boto3
import json
import os
from decimal import Decimal
from datetime import datetime
import re

# Create directory for exports
if not os.path.exists('dynamodb_exports'):
    os.makedirs('dynamodb_exports')

# Custom JSON encoder for DynamoDB types
class DynamoDBEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DynamoDBEncoder, self).default(obj)

def read_credentials_file(file_name):

    # get current file directory and append the file name
    file_path = os.path.join(os.path.dirname(__file__), file_name)
    print(f"Full path to credentials file: {file_path}")

    """Read AWS credentials from file using regex instead of configparser"""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Extract credentials using regex
    access_key = re.search(r'aws_access_key_id\s*=\s*([^\s]+)', content)
    secret_key = re.search(r'aws_secret_access_key\s*=\s*([^\s]+)', content)
    session_token = re.search(r'aws_session_token\s*=\s*([^\s]+)', content)
    
    credentials = {}
    if access_key:
        credentials['aws_access_key_id'] = access_key.group(1)
    if secret_key:
        credentials['aws_secret_access_key'] = secret_key.group(1)
    if session_token:
        credentials['aws_session_token'] = session_token.group(1)
    
    # Verify we have the required credentials
    if 'aws_access_key_id' not in credentials or 'aws_secret_access_key' not in credentials:
        raise ValueError(f"Required credentials not found in {file_path}")
    
    return credentials

def export_tables():
    print("=== AWS ACADEMY EXPORT ===")
    
    # Get AWS Academy credentials from file
    try:
        academy_creds = read_credentials_file('aws-academy-cred.txt')
        
        # Initialize AWS clients with Academy credentials
        academy_session = boto3.Session(
            aws_access_key_id=academy_creds['aws_access_key_id'],
            aws_secret_access_key=academy_creds['aws_secret_access_key'],
            aws_session_token=academy_creds.get('aws_session_token'),
            region_name='us-east-1'
        )
        
        dynamodb = academy_session.resource('dynamodb')
        dynamodb_client = academy_session.client('dynamodb')
        
        print(f"Successfully authenticated with AWS Academy credentials")
    except Exception as e:
        print(f"Error reading or using AWS Academy credentials: {e}")
        return
    
    # Tables to export
    tables = [
        'crypto_coins_gecko',
        'crypto_market_prices',
        'reddit_posts',
    ]
    
    # Export schema and data
    for table_name in tables:
        print(f"\nExporting table: {table_name}")
        
        # Export schema
        try:
            response = dynamodb_client.describe_table(TableName=table_name)
            table_def = response['Table']
            
            # Remove runtime-specific fields
            for field in ['TableStatus', 'CreationDateTime', 'TableArn', 
                         'ItemCount', 'TableSizeBytes', 'LatestStreamArn', 
                         'LatestStreamLabel']:
                if field in table_def:
                    table_def.pop(field)
            
            # Handle provisioned throughput
            if 'ProvisionedThroughput' in table_def:
                table_def.pop('ProvisionedThroughput')
            
            # Handle GSI provisioned throughput
            if 'GlobalSecondaryIndexes' in table_def:
                for gsi in table_def['GlobalSecondaryIndexes']:
                    if 'ProvisionedThroughput' in gsi:
                        gsi.pop('ProvisionedThroughput')
            
            # Save schema
            with open(f'dynamodb_exports/{table_name}_schema.json', 'w') as f:
                json.dump(table_def, f, indent=2, cls=DynamoDBEncoder)
            
            print(f"Exported schema for {table_name}")
        except Exception as e:
            print(f"Error exporting schema: {e}")
        
        # Export data
        try:
            table = dynamodb.Table(table_name)
            response = table.scan()
            items = response['Items']
            
            # Handle pagination for large tables
            while 'LastEvaluatedKey' in response:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                items.extend(response['Items'])
            
            with open(f'dynamodb_exports/{table_name}_data.json', 'w') as f:
                json.dump(items, f, indent=2, cls=DynamoDBEncoder)
            
            print(f"Exported {len(items)} items from {table_name}")
        except Exception as e:
            print(f"Error exporting data: {e}")
    
    print("\nExport complete!")

def import_tables():
    print("\n=== PERSONAL AWS ACCOUNT IMPORT ===")
    
    # Get personal AWS credentials
    try:
        personal_creds = read_credentials_file('aws-personal-cred.txt')
        
        # Initialize AWS clients with personal credentials
        personal_session = boto3.Session(
            aws_access_key_id=personal_creds['aws_access_key_id'],
            aws_secret_access_key=personal_creds['aws_secret_access_key'],
            region_name='us-east-1'
        )
        
        dynamodb = personal_session.resource('dynamodb')
        dynamodb_client = personal_session.client('dynamodb')
        
        print("Successfully authenticated with personal AWS credentials")
    except Exception as e:
        print(f"Error reading or using personal AWS credentials: {e}")
        return
    
    # Step 1: Create tables
    schema_files = [f for f in os.listdir('dynamodb_exports') if f.endswith('_schema.json')]
    
    for schema_file in schema_files:
        table_name = schema_file.replace('_schema.json', '')
        print(f"\nCreating table: {table_name}")
        
        try:
            # Load schema 
            with open(os.path.join('dynamodb_exports', schema_file), 'r') as f:
                table_def = json.load(f)
            
            # Check if table already exists
            try:
                dynamodb_client.describe_table(TableName=table_name)
                print(f"Table {table_name} already exists, skipping creation")
                continue
            except dynamodb_client.exceptions.ResourceNotFoundException:
                pass
            
            # Prepare create table request
            create_params = {
                'TableName': table_name,
                'AttributeDefinitions': table_def['AttributeDefinitions'],
                'KeySchema': table_def['KeySchema'],
                'BillingMode': 'PAY_PER_REQUEST'  # Use on-demand capacity
            }
            
            # Add GSIs if present
            if 'GlobalSecondaryIndexes' in table_def:
                create_params['GlobalSecondaryIndexes'] = table_def['GlobalSecondaryIndexes']
            
            # Create table
            response = dynamodb_client.create_table(**create_params)
            print(f"Created table: {table_name}")
            
            # Wait for table to become active
            print(f"Waiting for table {table_name} to become active...")
            waiter = dynamodb_client.get_waiter('table_exists')
            waiter.wait(TableName=table_name)
            print(f"Table {table_name} is now active")
            
        except Exception as e:
            print(f"Error creating table {table_name}: {e}")
    
    # Step 2: Import data
    data_files = [f for f in os.listdir('dynamodb_exports') if f.endswith('_data.json')]
    
    for data_file in data_files:
        table_name = data_file.replace('_data.json', '')
        print(f"\nImporting data into {table_name}")
        
        try:
            # Load data
            with open(os.path.join('dynamodb_exports', data_file), 'r') as f:
                items = json.load(f)
            
            # Get table
            table = dynamodb.Table(table_name)
            
            # Write items in batches (maximum 25 per batch)
            batch_size = 25
            imported = 0
            
            for i in range(0, len(items), batch_size):
                batch = items[i:i+batch_size]
                
                # Convert floats back to Decimal
                with table.batch_writer() as batch_writer:
                    for item in batch:
                        # Convert float to Decimal for DynamoDB
                        item_for_dynamodb = json.loads(
                            json.dumps(item),
                            parse_float=Decimal
                        )
                        batch_writer.put_item(Item=item_for_dynamodb)
                
                imported += len(batch)
                print(f"Imported {imported}/{len(items)} items")
            
            print(f"Completed importing data into {table_name}")
            
        except Exception as e:
            print(f"Error importing data to {table_name}: {e}")
    
    print("\nData import complete!")

def main():
    print("DynamoDB Export/Import Tool")
    print("1. Export tables from AWS Academy")
    print("2. Import tables to personal AWS account")
    print("3. Do both (export then import)")
    
    choice = input("Enter your choice (1-3): ")
    
    if choice == '1':
        export_tables()
    elif choice == '2':
        import_tables()
    elif choice == '3':
        export_tables()
        import_tables()
    else:
        print("Invalid choice!")

if __name__ == "__main__":
    main()