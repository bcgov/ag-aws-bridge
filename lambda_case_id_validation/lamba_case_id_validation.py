import boto3
import os
import json
from botocore.exceptions import ClientError
from datetime import datetime
import requests  # Added missing import

def lambda_case_id_validation(event, context):  # Fixed function name typo (lamba -> lambda)
    # Extract bridge_job_id from event
    bridge_job_id = event.get('bridge_job_id')
    
    region = "ca-central-1"
    if not bridge_job_id:
        raise ValueError('bridge_job_id is required')
    
    try:
        # Initialize AWS clients
        ssm_client = boto3.client('ssm')
        env = os.environ.get("ENV_STAGE")
        
        sqs = boto3.client('sqs')

        # Retrieve parameters from Parameter Store
        table_name_param = ssm_client.get_parameter(
            Name=f"/prime-rms/{env}/dynamodb-table",
            WithDecryption=False)

        isl_endpoint_url = ssm_client.get_parameter(
            Name=f"/prime-rms/{env}/isl_endpoint_url",
            WithDecryption=False)['Parameter']['Value']  # Added value extraction
        
        isl_endpoint_secret = ssm_client.get_parameter(
            Name=f"/prime-rms/{env}/isl_endpoint_secret",
            WithDecryption=True)['Parameter']['Value']  # Added value extraction
        
        transfer_sqs_name = ssm_client.get_parameter(
            Name=f"/prime-rms/{env}/sqs-queue-q-file-transfer",
            WithDecryption=False)
        
        dynamodb = boto3.resource('dynamodb', region_name=region, use_ssl=True)
        
        table_name = table_name_param['Parameter']['Value']

        # Get the queue URL
        response = sqs.get_queue_url(QueueName=transfer_sqs_name['Parameter']['Value'])
        queue_url = response['QueueUrl']
        
        # Get DynamoDB table
        table = dynamodb.Table(table_name)

        # Read from DynamoDB - Example: Get item by primary key
        try:
            dynamodb_response = table.get_item(
                Key={
                    'bridge-job-id': bridge_job_id  # Fixed: use variable instead of string
                }
            )
            
            # Store DynamoDB result in a variable
            if 'Item' in dynamodb_response:
                dynamodb_data = dynamodb_response['Item']
            else:
                dynamodb_data = None  # Item not found
                
            print(f"DynamoDB data: {dynamodb_data}")
            caseNumber = dynamodb_data.get('source_case_number') if dynamodb_data else None
            agencyCode = dynamodb_data.get('source_agency') if dynamodb_data else None

        except ClientError as e:
            print(f"DynamoDB error: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': f"DynamoDB error: {str(e)}"})
            }
       
        try:
            # API call 
            api_url = isl_endpoint_url + "/ccm-justin-in-adapter/getPrimaryCaseByAgencyNo"

            # Make API call
            headers = {
                'Authorization': isl_endpoint_secret,
                'Content-Type': 'application/json',
                'User-Agent': 'Lambda-Function/1.0',
                'agencyIdCode': agencyCode,
                'agencyFileNumber': caseNumber
            }

            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            api_result = response.json()
            caseId = ""

            if "Inactive" in api_result:
                print("Found inactive case")
                  # Update item in DynamoDB
                update_response = table.update_item(
                    Key={
                        'bridge-job-id': bridge_job_id  # Fixed key name consistency
                    },
                    UpdateExpression='SET bridge_job_status = :status,dems_caseId= :caseId, bridge_job_updated_utc = :updated_val, bridge_job_details= :rowMsg',
                     ExpressionAttributeValues={
                            ':status': 'ERROR',
                            ':caseId' : caseId,
                            'updated_val':datetime.utcnow().isoformat(),
                            'rowMsg' : api_result

                    },
                   
                    ReturnValues='UPDATED'
                )
            else:
                caseId = api_result

            if caseId != "":
                # Update item in DynamoDB
                update_response = table.update_item(
                    Key={
                        'bridge-job-id': bridge_job_id  # Fixed key name consistency
                    },
                    UpdateExpression='SET bridge_job_status = :status,dems_caseId= :caseId, bridge_job_updated_utc = :updated_val, bridge_job_details= :rowMsg',
                     ExpressionAttributeValues={
                            ':status': 'VALID-CASE',
                            ':caseId' : caseId,
                            'updated_val':datetime.utcnow().isoformat(),
                            'rowMsg' : 'Lookup CaseId completed'

                    },
                   
                    ReturnValues='UPDATED'
                )
                
                # If you want to verify the update was successful
                if 'Attributes' not in update_response:
                    raise Exception(f'Failed to update job {bridge_job_id}')
                
                # Initialize the SQS client
                sqs = boto3.client("sqs")

                # Define the SQS queue URL (replace with your queue URL)
                QUEUE_URL = queue_url

                 # Example message to send (can come from the event or elsewhere)
                message_body = {
                    "id": bridge_job_id,
                    "message": "Case lookup successful",
                    "timestamp": "2025-03-21T12:00:00Z"
                }
                  # Send the message to the SQS queue
                response = sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(message_body),  # Convert dict to JSON string
                MessageAttributes={                   # Optional: Add metadata
                    "Source": {
                        "DataType": "String",
                        "StringValue": "LambdaFunction"
                    }
                }
            )
        
                # Since we're not returning a response, we just return a simple success
                return {
                    'statusCode': 200,
                    'body': json.dumps({'message': f'Successfully updated job {bridge_job_id}'})
                }
            
        except requests.RequestException as e:  # Added specific exception handling for requests
            return {
                'statusCode': 500,
                'body': json.dumps({'error': f"API call error: {str(e)}"})
            }
    
    except ClientError as e:
        # Handle AWS-specific errors
        error_message = f"AWS Error: {str(e)}"
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }
    except Exception as e:
        # Handle general errors
        error_message = f"Error: {str(e)}"
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }

# Example usage (for testing locally):
if __name__ == "__main__":
    test_event = {
        'bridge_job_id': 'job123'
    }
    print(lambda_case_id_validation(test_event, None))  # Fixed function name in test