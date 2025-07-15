# ==================================================
# FILE: README.md
# ==================================================

"""
# Lambda Structured Logger Layer

A reusable Lambda Layer providing consistent structured logging across all Lambda functions.

## Installation

1. Run `bash build_layer.sh` to create the layer zip file
2. Upload `lambda-structured-logger-layer.zip` to AWS Lambda Layers
3. Add the layer to Lambda functions
4. Set the layer as compatible with the Python runtime (3.8, 3.9, 3.10, 3.11)

## Usage in Lambda Functions

```python
from lambda_structured_logger import LambdaStructuredLogger, LogStatus, LogLevel

def lambda_handler(event, context):
    logger = LambdaStructuredLogger()
    
    job_id = event.get('job_id')
    
    logger.log_start("my_operation", job_id=job_id)
    
    try:
        # BRIDGE business logic here
        result = do_something()
        
        logger.log_success(
            event="my_operation",
            message="Operation completed successfully",
            job_id=job_id
        )
        
        return {"statusCode": 200, "body": "Success"}
        
    except Exception as e:
        logger.log_error("my_operation", e, job_id=job_id)
        return {"statusCode": 500, "body": "Error"}
```

## Environment Variables

The logger automatically reads these environment variables:
- `AWS_LAMBDA_FUNCTION_NAME` (set by Lambda)
- `ENV_STAGE` (set by you, e.g., "dev", "staging", "prod")
- `_X_AMZN_TRACE_ID` (set by Lambda)
- `AWS_LAMBDA_FUNCTION_VERSION` (set by Lambda)

## CloudWatch Queries

```sql
# Track a specific job
fields @timestamp, lambda_function, event, status, message
| filter job_id = "job_id"
| sort @timestamp asc

# Find all errors
fields @timestamp, lambda_function, event, message, error_details
| filter status = "FAILURE"
| sort @timestamp desc
```
"""

# ==================================================
# TERRAFORM EXAMPLE (Optional)
# ==================================================

"""
# terraform/lambda_layer.tf

resource "aws_lambda_layer_version" "structured_logger" {
  filename            = "../lambda-structured-logger-layer.zip"
  layer_name          = "lambda-structured-logger"
  description         = "Structured logging for Lambda functions"
  
  compatible_runtimes = ["python3.8", "python3.9", "python3.10", "python3.11"]
  
  source_code_hash = filebase64sha256("../lambda-structured-logger-layer.zip")
}

# Example Lambda function using the layer
resource "aws_lambda_function" "evidence_processor" {
  filename         = "evidence_processor.zip"
  function_name    = "evidence-processor"
  role            = aws_iam_role.lambda_role.arn
  handler         = "lambda_function.lambda_handler"
  runtime         = "python3.10"
  
  layers = [aws_lambda_layer_version.structured_logger.arn]
  
  environment {
    variables = {
      ENV_STAGE = "production"
    }
  }
}
"""

# ==================================================
# DEPLOYMENT INSTRUCTIONS
# ==================================================

"""
STEP-BY-STEP DEPLOYMENT:

1. Create the layer structure:
   mkdir -p lambda-structured-logger-layer/python/lambda_structured_logger

2. Copy the files:
   - Copy __init__.py to python/lambda_structured_logger/
   - Copy logger.py to python/lambda_structured_logger/
   - Copy enums.py to python/lambda_structured_logger/

3. Build the layer:
   bash build_layer.sh

4. Upload to AWS:
   - Go to AWS Lambda Console
   - Click "Layers" in the left navigation
   - Click "Create layer"
   - Upload lambda-structured-logger-layer.zip
   - Set compatible runtimes (Python 3.8+)

5. Add to Lambda functions:
   - Edit each Lambda function
   - Scroll to "Layers" section
   - Click "Add a layer"
   - Select "Custom layers"
   - Choose structured logger layer

6. Update Lambda code:
   from lambda_structured_logger import LambdaStructuredLogger
"""