import json
import pg8000.native
import os

# Note: AWS Lambda uses environment variables, not .env files
# Set these in Lambda Console: Configuration -> Environment variables

# ----------------------------
# Function: Redshift connection setup
# ----------------------------
def connect_redshift():
    # Get environment variables with validation
    user = os.getenv("redshift_user")
    password = os.getenv("redshift_password")
    host = os.getenv("redshift_host")
    database = os.getenv("redshift_db")
    
    # Validate required environment variables
    if not all([user, password, host, database]):
        raise ValueError("Missing required Redshift environment variables")
    
    conn = pg8000.native.Connection(
        user=user,
        password=password,
        host=host,
        database=database,
        port=5439
    )
    return conn

# ----------------------------
# Function: Load data from S3 to Redshift using COPY command
# ----------------------------
def s3_to_redshift(conn, iam_role, s3_bucket, s3_key):
    copy_command = f"""
    COPY user_data
    FROM 's3://{s3_bucket}/{s3_key}'
    IAM_ROLE '{iam_role}'
    FORMAT AS PARQUET;
    """

    try:
        print("Executing COPY command...")
        print(f"Loading: s3://{s3_bucket}/{s3_key}")
        conn.run(copy_command)
        print(f"✓ Data from {s3_key} loaded successfully into Redshift.")
        return True
    except Exception as e:
        print(f"✗ Error loading data: {str(e)}")
        raise  # Re-raise to handle in lambda_handler
    finally:
        if conn:
            conn.close()
            print("Connection closed.")

# ----------------------------
# Lambda Handler: Triggered when file is uploaded to S3
# ----------------------------
def lambda_handler(event, context):
    try:
        # Validate IAM role
        iam_role = os.getenv("iam_role")
        if not iam_role:
            raise ValueError("Missing IAM_ROLE environment variable")
        
        # Parse S3 event
        s3_bucket = event['Records'][0]['s3']['bucket']['name']
        s3_key = event['Records'][0]['s3']['object']['key']
        
        print(f"Lambda triggered by file upload: s3://{s3_bucket}/{s3_key}")
        
        # Only process Parquet files
        if s3_key.endswith('.parquet'):
            print("File is a Parquet file, processing...")
            conn = connect_redshift()
            s3_to_redshift(conn, iam_role, s3_bucket, s3_key)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Data loaded successfully',
                    'file': f's3://{s3_bucket}/{s3_key}'
                })
            }
        else:
            print(f"Skipping non-Parquet file: {s3_key}")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'File skipped (not a Parquet file)',
                    'file': s3_key
                })
            }
            
    except Exception as e:
        print(f"Lambda execution failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error processing file',
                'error': str(e)
            })
        }