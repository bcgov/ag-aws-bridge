import json
import boto3
#import requests
import urllib3
import urllib.parse
import os 
import time

from lambda_structured_logger import LambdaStructuredLogger, LogLevel, LogStatus
from datetime import datetime, timezone
from bridge_tracking_db_layer import DatabaseManager, StatusCodes, get_db_manager
from datetime import timedelta
from botocore.config import Config


def lambda_handler(event, context):
    #os.environ["OTEL_PYTHON_DISABLED"] = "true"

    # Initialize the logger
    logger = LambdaStructuredLogger()

 # Log the start of the function
    logger.log_start(
        event="Axon Case Detector Start",
        job_id=context.aws_request_id
    )

    # Initialize AWS SSM client
    config = Config(connect_timeout=5, retries={"max_attempts": 5, "mode": "standard"})
    ssm_client = boto3.client("ssm", region_name="ca-central-1", config=config)
    #logger.log(event="checkpoint", status=LogStatus.IN_PROGRESS, message="After [ssm init]")


    sqs = boto3.client('sqs')
    #logger.log(event="checkpoint", status=LogStatus.IN_PROGRESS, message="After [sqs init]")

    # Initialize database manager
    # Get environment stage from environment variable
    env_stage = os.environ.get('ENV_STAGE', 'dev-test')

   # logger.log(event="pre_db_init", status=LogStatus.IN_PROGRESS, message="Before DB manager init")


    db_manager = get_db_manager(env_param_in=env_stage)
    db_manager._initialize_pool()
    #logger.log(event="checkpoint", status=LogStatus.IN_PROGRESS, message="After [db manager init ]")

    #initialize HTTP Connection Pool
    http = urllib3.PoolManager()
    #logger.log(event="checkpoint", status=LogStatus.IN_PROGRESS, message="After [http manager init ]")
    
    # Collect SSM parameters
    try:
        # Define the parameter names you want to retrieve
        # 'host': f'/{env_stage}/bridge/tracking-db/host',
        parameter_names = [
            f'/{env_stage}/axon/api/get_cases_url_filter_path',
            f'/{env_stage}/axon/api/authentication_url',
            f'/{env_stage}/axon/api/base_url',
            f'/{env_stage}/axon/api/bearer',
            f'/{env_stage}/axon/api/client_secret',
            f'/{env_stage}/axon/api/client_id',
            f'/{env_stage}/axon/api/agency_id',
            f'/{env_stage}/axon/api/case_detector_interval_mins',
            f'/{env_stage}/bridge/sqs-queues/arn_q-axon-case-found'
        ]
        
        # Retrieve multiple parameters at once
        response = ssm_client.get_parameters(
            Names=parameter_names,
            WithDecryption=True  # Set to True for SecureString parameters
        )
        
        # Process the parameters into a dictionary
        parameters = {}
        for param in response['Parameters']:
            parameters[param['Name']] = param['Value']

        if response.get('InvalidParameters'):
            logger.log_error(event = "SSM Param Retrieval", error=none,job_id=context.aws_request_id)
            raise Exception(f"Failed to retrieve some parameters: {response['InvalidParameters']}")
        
        # Add API method
        parameters["method"] = "POST"
        api_url =  parameters[f'/{env_stage}/axon/api/base_url']  + parameters[f'/{env_stage}/axon/api/authentication_url']
        #https://bcps-dev.ca.evidence.com/api/oauth2/token
        # Get API bearer token
        try:
            payload = {
            "client_id": parameters[f'/{env_stage}/axon/api/client_id'],
            "grant_type": "client_credentials",
            "client_secret": parameters[f'/{env_stage}/axon/api/client_secret']
            }
            # Convert payload to URL-encoded format
            encoded_payload = urllib.parse.urlencode(payload).encode('utf-8')

            response = http.request(
                method='POST',
                url=api_url,
                body=encoded_payload,
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )
            logger.log_success(
                event="api_call",
                message="Bearer token retrieval success",
                job_id=context.aws_request_id,
                custom_metadata={"status_code": response.status, "url": api_url}
            )
            data = json.loads(response.data.decode('utf-8'))  # Decode response properly
            parameters[f'/{env_stage}/axon/api/bearer'] = data.get("access_token")
            
        except urllib3.exceptions.HTTPError as e:
            logger.log_error(
            event="api_call_failed",
            error=e,
            job_id=context.aws_request_id
            )
            return {
            'statusCode': 500,
            'body': json.dumps({
            'message': 'Error getting bearer token',
            'error': str(e)
            })
            }
        
        # Prepare headers for the GET request (assuming bearer token authentication)
        headers = {
            'Authorization': f"Bearer {parameters[f'/{env_stage}/axon/api/bearer']}",
            'Content-Type': 'application/json'
        }
        # Make the GET request to the API endpoint
        api_url = parameters[f'/{env_stage}/axon/api/base_url'] + 'api/v2/agencies/' +  parameters[f'/{env_stage}/axon/api/agency_id'] + '/cases'
        logger.log(event="created api url ", status=LogStatus.IN_PROGRESS, message="API URL constructed : " + api_url)

        # Get current UTC time
        current_utc_time = datetime.now(timezone.utc)
        #set date/time for testing
        #current_utc_time = datetime(2025, 6, 12, 19, 5, 0, tzinfo=timezone.utc)
        current_utc_time_str = current_utc_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        # Get interval to use
        case_detector_interval_mins = int(parameters[f'/{env_stage}/axon/api/case_detector_interval_mins'])
        #case_detector_interval_mins  = 2880
        # Substract 5 minutes to get second UTC time
        fivemins_past = current_utc_time - timedelta(minutes=case_detector_interval_mins)
        fivemins_past_str = fivemins_past.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        
        # make filter string
        filter_string = f"createdOn in {fivemins_past_str} to {current_utc_time_str}"

        transferJobStatus = ""
        
        db_health_Check = db_manager.health_check()
        logger.log(event="checkpoint", status=LogStatus.IN_PROGRESS, message="After [db manager healthcheck, db healthy : " + str(db_health_Check["healthy"]))
        try:
            params = {
                "filter": filter_string
            }
            logger.log(event="filter string ", status=LogStatus.IN_PROGRESS, message="filter string : " + filter_string)
            start = time.perf_counter()
            response = http.request('GET', api_url, fields=params,headers = headers, timeout=urllib3.Timeout(connect=5, read=10))
           
            if response.status >= 400: raise Exception(f"HTTP error: {response.status}")

            # Get response time
            response_time = time.perf_counter() - start

            logger.log_api_call(event="call to Axon Get Cases", url=api_url, method="GET", status_code= response.status, 
            response_time = response_time,   job_id=context.aws_request_id)

            # Log success and return the response
            logger.log_success(
                event="api_call",
                message="Call to get cases successful",
                job_id = context.aws_request_id,
                custom_metadata={"status_code": response.status, "url": api_url}
            )
            queue_url = parameters[f'/{env_stage}/bridge/sqs-queues/arn_q-axon-case-found']
              # Optionally set queue attributes first (e.g., for KMS encryption) - run this only once or as needed
            try:
                sqs.set_queue_attributes(
                    QueueUrl=queue_url,
                    Attributes={
                        'KmsMasterKeyId': 'alias/aws/sqs'  # Correct alias for default AWS-managed KMS key
                    }
                    )
            except Exception as e:
                print(f"Error setting queue attributes: {e}")

            if response.status == 200:
                # Parse JSON response
                json_data = json.loads(response.data.decode('utf-8'))
                print(json_data)
                 # Extract top-level meta information
                meta = json_data.get("meta", {})
                offset = meta.get("offset")
                limit = meta.get("limit")
                count = int(meta.get("count"))

                logger.log(event="case count found  ", status=LogStatus.IN_PROGRESS, message="count : " + str(count))
                db_health_Check = db_manager.health_check()
                logger.log(event="checkpoint", status=LogStatus.IN_PROGRESS, message="After [db manager healthcheck 2 , db healthy : " + str(db_health_Check["healthy"]))
                # get status code 
                transferJobStatus = db_manager.get_status_code_by_value("NEW-EVIDENCE-SHARE")
                if transferJobStatus:
                    statusIdentifier = str(transferJobStatus["identifier"])
                
                if count >= 1:
                    data = json_data.get("data", [])
                     # Get response time
                    #response_time = time.perf_counter() - start
            
                    logger.log_api_call(event="call to Axon Get Cases successful. Found at least 1 case", url=api_url, method="GET", status_code= response.status, 
                    response_time = response_time,   job_id=context.aws_request_id)

                    for item in data:
                        # Extract fields from each item
                        item_type = item.get("type")
                        item_id = item.get("id")
                        attributes = item.get("attributes", {})

                        # Extract attributes
                        title = attributes.get("title")
                        description = attributes.get("description")
                        created_on = attributes.get("createdOn")
                        status = attributes.get("status")
                        owner = attributes.get("owner", {})
                        owner_id = owner.get("id")
                        owner_agency = owner.get("relationships", {}).get("agency", {}).get("data", {}).get("id")
                        sourceCaseLastModified = attributes.get("lastModified")
                        # Extract caseSharedFrom (list)
                        case_shared_from = attributes.get("caseSharedFrom", [])

                      

                        queryParams = {"job_id" : context.aws_request_id, "job_created_utc" : current_utc_time, 
                        "source_agency" : case_shared_from, "source_case_id" : item_id, "job_status_code" : statusIdentifier,
                        "source_system" : "Axon", "source_case_evidence_count_total" : count,
                         "source_case_title" : title, "last_modified_process": "lambda: axon case detector", "source_case_last_modified_utc" : sourceCaseLastModified ,
                          "last_modified_utc" : current_utc_time}
                        
                        db_manager.create_evidence_transfer_job(job_data=queryParams)
                        logger.log_database_update_jobs(job_id=context.aws_request_id,status=response.status, rows_affected=count, response_time_ms=response_time)

                        # Send SQS Message            
                        try:
                            logger.log(event="calling SQS to add msg ", status=LogStatus.IN_PROGRESS, message="Trying to call SQS ...")
                        # Send a message to the queue
                            response = sqs.send_message(
                                QueueUrl=queue_url,
                                MessageBody='Cases detected, details stored',
                                DelaySeconds=0,  # Deliver after 5 seconds
                                MessageGroupId="AXON-" + item_id,
                                MessageAttributes={
                                    'Job_id': {
                                        'DataType': 'String',
                                        'StringValue': context.aws_request_id
                                    },
                                    'Source_case_title': {
                                        'DataType': 'String',
                                        'StringValue': title
                                    }      
                                }
                            )
                           # logger.log_sqs_message_sent(queue_url = queue_url, message_id=response, )

                        except Exception as e:
                            print(f"Error sending message: {e}")
                    
                    result = {"statusCode": 200, "body": "Success calling API, at least 1 result found."}
                    return result
                else:
                # Get response time
                #response_time = response.elapsed.total_seconds()
                    logger.log_api_call(event="call to Axon Get Cases successful. Found no cases", url=api_url, method="GET", status_code= response.status, 
                    response_time = response_time,   job_id=context.aws_request_id)
                   
                    result = {"statusCode": 200, "body": "Success calling API, no results found."}
                    return result
            
        except Exception as e:
            logger.log_error(
                event="api_call_failed",
                error=e,
               job_id = context.aws_request_id
            )
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Error making GET request',
                    'error': str(e)
                })
            }

    except Exception as e:
        logger.log_error(
            event="SSM Param Retrieval",
            error=e,
            job_id=context.aws_request_id
        )
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error retrieving parameters',
                'error': str(e)
            })
        }
    finally:
    # Ensure connections are returned to the pool
        if "db_manager" in locals():
            db_manager.close_connections()