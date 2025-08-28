import json
import boto3
import urllib3
import urllib.parse
import os
import time
from datetime import datetime, timezone, timedelta
from botocore.config import Config

from lambda_structured_logger import LambdaStructuredLogger, LogLevel, LogStatus
from bridge_tracking_db_layer import DatabaseManager, StatusCodes, get_db_manager

def lambda_handler(event, context):
    # Initialize the logger
    logger = LambdaStructuredLogger()

    # Log the start of the function
    logger.log_start(
        event="Verify Dems Case Start",
        job_id=context.aws_request_id
    )

    # Get environment stage from environment variable
    env_stage = os.environ.get('ENV_STAGE', 'dev-test')

    # Initialize AWS clients with custom config
    config = Config(connect_timeout=5, retries={"max_attempts": 5, "mode": "standard"})
    ssm_client = boto3.client("ssm", region_name="ca-central-1", config=config)
    sqs = boto3.client('sqs', region_name="ca-central-1", config=config)
    dynamodb = boto3.resource("dynamodb", region_name="ca-central-1")
    agency_code_table = dynamodb.Table("agency-lookups")
    db_manager = get_db_manager(env_param_in=env_stage)
    db_manager._initialize_pool()

    # Initialize database manager
    db_manager = get_db_manager(env_param_in=env_stage)
    db_manager._initialize_pool()

    # Initialize HTTP Connection Pool with retries and timeout
    http = urllib3.PoolManager(
        timeout=urllib3.Timeout(connect=5.0, read=10.0),
        retries=urllib3.Retry(total=3, backoff_factor=1)
    )

    # Define SSM parameter names (fixed trailing spaces and added missing ones for Axon API)
    parameter_names = [
        f'/{env_stage}/isl_endpoint_url',
        f'/{env_stage}/isl_endpoint_secret',
        f'/{env_stage}/bridge/sqs-queues/arn_q-transfer-exception',
        f'/{env_stage}/bridge/sqs-queues/arn_q-axon-case-detail',
        f'/{env_stage}/bridge/tracking-db/connection-string',
        f'/{env_stage}/bridge/sqs-queues/arn_q-axon-case-found',
        f'/{env_stage}/axon/api/client_id',
        f'/{env_stage}/axon/api/client_secret',
        f'/{env_stage}/axon/api/token_url'  # Assuming a parameter for the token URL; add if needed
    ]

    try:
        # Retrieve multiple parameters at once
        ssm_response = ssm_client.get_parameters(
            Names=parameter_names,
            WithDecryption=True
        )
        
        # Process into a dictionary
        parameters = {param['Name']: param['Value'] for param in ssm_response['Parameters']}

        if ssm_response.get('InvalidParameters'):
            invalid_params = ssm_response['InvalidParameters']
            logger.log_error(
                event="SSM Param Retrieval",
                error=f"Failed to retrieve parameters: {invalid_params}",
                job_id=context.aws_request_id
            )
            raise ValueError(f"Failed to retrieve some parameters: {invalid_params}")
        
        logger.log_success(
            event="SSM Param Retrieval",
            message="Parameters collected successfully",
            job_id=context.aws_request_id,
            custom_metadata={"parameter_names": parameter_names}
        )
    except Exception as e:
        logger.log_error(
            event="SSM Param Retrieval Failed",
            error=str(e),
            job_id=context.aws_request_id
        )
        raise

    # Fetch bearer token
    try:
        token_url = parameters[f'/{env_stage}/axon/api/token_url']  # Ensure this parameter exists
        payload = {
            "client_id": parameters[f'/{env_stage}/axon/api/client_id'],
            "grant_type": "client_credentials",
            "client_secret": parameters[f'/{env_stage}/axon/api/client_secret']
        }
        encoded_payload = urllib.parse.urlencode(payload).encode('utf-8')

        token_response = http.request(
            method='POST',
            url=token_url,
            body=encoded_payload,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )

        if token_response.status != 200:
            raise ValueError(f"Token retrieval failed with status {token_response.status}")

        data = json.loads(token_response.data.decode('utf-8'))
        bearer_token = data.get("access_token")
        if not bearer_token:
            raise ValueError("No access token in response")

        logger.log_success(
            event="Bearer Token Retrieval",
            message="Token retrieved successfully",
            job_id=context.aws_request_id,
            custom_metadata={"status_code": token_response.status, "url": token_url}
        )
    except Exception as e:
        logger.log_error(
            event="Bearer Token Retrieval Failed",
            error=str(e),
            job_id=context.aws_request_id
        )
        raise

    # Process SQS messages
    try:
        queue_url = parameters[f'/{env_stage}/bridge/sqs-queues/arn_q-axon-case-found']

        sqs_response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
            VisibilityTimeout=30
        )

        if 'Messages' not in sqs_response:
            logger.log_info(
                event="SQS Poll",
                message="No messages in queue",
                job_id=context.aws_request_id
            )
            return {'statusCode': 200, 'body': 'No messages to process'}

        for message in sqs_response['Messages']:
            try:
                body = json.loads(message['Body'])
                job_id = body.get("job_id")
                case_title = body.get("Source_case_title")

                if not job_id or not case_title:
                    raise ValueError("Missing job_id or Source_case_title in message")

                # Parse agency from case_title
                parts = case_title.split('-')
                if len(parts) < 2:
                    raise ValueError("Invalid case_title format")

                rms_jur_id = parts[0].strip()
                agency_file_number = '-'.join(parts[1:]).strip()  # Handle cases with multiple '-'

                # Lookup agency code in DynamoDB
                logger.log_info(
                    event="Agency Code Lookup",
                    status=LogStatus.IN_PROGRESS,
                    message="Retrieving agency code...",
                    job_id=job_id
                )
                dynamo_response = agency_code_table.get_item(
                    Key={'rmsJurId': rms_jur_id}
                )

                item = dynamo_response.get('Item')
                if not item:
                    logger.log_error(
                        event="Agency Code Lookup Failed",
                        error=f"No item found for rmsJurId: {rms_jur_id}",
                        job_id=job_id
                    )
                    # Optionally send to DLQ or continue
                    continue

                agency_id_code = item.get('bcpsAgencyIdCode', '')
                sub_agency_yn = item.get('subAgencyYN', 'N')
                sub_agencies = item.get('subAgencies', [])  # Assuming subAgencies is a list of strings

                # Log successful lookup
                current_timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
                logger.log_info(
                    event="Axon Case Agency Lookup",
                    message="Agency prefix lookup successful",
                    job_id=job_id,
                    custom_metadata={
                        "rms_jur_id": rms_jur_id,
                        "agency_id_code": agency_id_code,
                        "agency_file_number": agency_file_number
                    }
                )

                # Call DEMS API
                dems_api_url = parameters[f'/{env_stage}/isl_endpoint_url']
                found_dems_case = False
                agency_codes_to_try = [agency_id_code]

                if sub_agency_yn == 'Y' and sub_agencies:
                    agency_codes_to_try.extend(sub_agencies)

                for code in agency_codes_to_try:
                    headers = {
                        'Authorization': f"Bearer {bearer_token}",
                        'agencyIdCode': code,
                        'agencyFileNumber': agency_file_number
                    }

                    logger.log_info(
                        event="DEMS API Call",
                        status=LogStatus.IN_PROGRESS,
                        message=f"Calling DEMS API with agencyIdCode: {code}",
                        job_id=job_id
                    )

                    start_time = time.perf_counter()
                    api_response = http.request(
                        'GET',
                        dems_api_url,
                        headers=headers
                    )
                    response_time = time.perf_counter() - start_time
                   
                    logger.log_api_call(
                        event="DEMS ISL Get Cases",
                        url=dems_api_url,
                        method="GET",
                        status_code=api_response.status,
                        response_time=response_time,
                        job_id=job_id
                    )

                    if api_response.status == 200:
                        dems_case_id = api_response.data.decode('utf-8').strip()
                        if dems_case_id:
                            db_manager.set_dems_case(job_id, dems_case_id, "Verify Rcc Dems Case")
                            found_dems_case = True
                            logger.log_success(
                                event="DEMS Case Found",
                                message=f"DEMS case ID: {dems_case_id}",
                                job_id=job_id
                            )
                            break
                    elif api_response.status >= 400:
                        logger.log_error(
                            event="DEMS API Error",
                            error=f"HTTP error: {api_response.status}",
                            job_id=job_id
                        )

                if not found_dems_case:
                    logger.log(
                        event="axonRccAndDemsCaseValidator",
                        status = "ERROR",
                        message="Agency prefix lookup unsuccessful - not matched",
                        job_id=job_id,
                        additional_info={
                            "rms_jur_id" : rms_jur_id,
                            "agencyFileNumber" : agency_file_number
                        }
                    )
                    update_job_status = db_manager.get_status_code_by_value(value="INVALID-AGENCY-IDENTIFIER")
                    if update_job_status:
                        statusIdentifier = str(update_job_status["identifier"])
                        db_manager.update_job_status(job_id=job_id,status_code=statusIdentifier,job_msg="",last_modified_process="lambda: rcc and dems case validator")


                    # Optionally send to exception queue
                update_job_status = db_manager.get_status_code_by_value(value="VALID-CASE")
                if update_job_status:
                    statusIdentifier = str(update_job_status["identifier"])
                    db_manager.update_job_status(job_id=job_id,status_code=statusIdentifier,job_msg="",last_modified_process="lambda: rcc and dems case validator")

                # create new message on case details queue
                queue_url = parameters[f'/{env_stage}/bridge/sqs-queues/arn_q-axon-case-detail']
                try:
                        logger.log(event="calling SQS to add msg ", status=LogStatus.IN_PROGRESS, message="Trying to call SQS ...")
                        # Send a message to the queue
                        response = sqs.send_message(
                                QueueUrl=queue_url,
                                MessageBody='Cases detected, details stored',
                                DelaySeconds=0,  # Deliver after 5 seconds
                                MessageGroupId="axon-evidence-transfer",
                                MessageDuplicationId=job_id,
                                MessageAttributes={
                                    'Job_id': {
                                        'DataType': 'String',
                                        'StringValue': job_id if 'job_id' in locals() else context.aws_request_id
                                    },
                                    'Source_case_title': {
                                        'DataType': 'String',
                                        'StringValue': case_title
                                    }      
                                }
                            )
                        timestamp = datetime.datetime.now()
                        logger.log_sqs_message_sent(queue_url = queue_url, message_id=response, message_body={
                            "timestamp" : timestamp.isoformat(), "level": "INFO", "function" :"axonRccAndDemsCaseValidator",
                            "event" : "SQSMessageQueued", "message" : "Queued message for Axon Case Detail and Evidence Filter",
                            "job_id" : job_id,
                            "source_case_title" : case_title,
                            "additional_info" : {
                                "target_queue" : "q-axon-case-detail.fifo",
                                "message_group_id" : job_id,
                                "deduplication_id" : "file-" + response 
                            }
                           } )

                except Exception as e:
                    print(f"Error sending message: {e}")

            except Exception as msg_err:
                logger.log_error(
                    event="Message Processing Failed",
                    error=str(msg_err),
                    job_id=job_id if 'job_id' in locals() else context.aws_request_id
                )
                # Optionally: do not delete message or send to DLQ

    except Exception as e:
        logger.log_error(
            event="SQS Processing Failed",
            error=str(e),
            job_id=context.aws_request_id
        )
        raise

    # Log the end of the function
    logger.log_end(
        event="Verify Dems Case End",
        job_id=context.aws_request_id
    )

    return {'statusCode': 200, 'body': 'Processing complete'}