# Configure the AWS provider
provider "aws" {
  region = var.region  # Change to your preferred region
}


# Create SSM Parameter


#/prime-rms/dev/isl_endpoint_secret
resource "aws_ssm_parameter" "isl_endpoint_api_key" {
  name  = "/prime-rms/${var.env}/isl_endpoint_secret"
  type  = "SecureString"  # Can be String, StringList, or SecureString
  value = var.dems_isl_api_key
  
  # Optional: Add description
  description = "DEMS ISL API Endpoint credentials (encrypted value)"
}


resource "aws_ssm_parameter" "isl_endpoint_url" {
  name  = "/prime-rms/${var.env}/isl_endpoint_url"
  type  = "String"  # Can be String, StringList, or SecureString
  value = var.dems_isl_api_url
  
  # Optional: Add description
  description = "DEMS ISL API Endpoint URL"
}


resource "aws_ssm_parameter" "sqs_file_transfer" {
  name  = "/prime-rms/${var.env}/sqs-queue-q-file-transfer"
  type  = "String"  # Can be String, StringList, or SecureString
  value = var.sqs_file_transfer_name
  
  # Optional: Add description
  description = "SQS File Transfer name"
}

resource "aws_ssm_parameter" "case_id_dynamo_table_name" {
  name  = "/prime-rms/${var.env}/dynamodb-table"
  type  = "String"  # Can be String, StringList, or SecureString
  value = var.case_id_dynamo_table_name
  
  # Optional: Add description
  description = "Case Id Dynamo Table name"
}


# Create an IAM role for the Lambda function
resource "aws_iam_role" "lambda2_exec_role" {
  name = "lambda2_execution_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

# Attach the AWS Lambda basic execution policy to the role
resource "aws_iam_role_policy_attachment" "lambda_policy" {
  role       = aws_iam_role.lambda2_exec_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Create a custom policy for SSM access
resource "aws_iam_policy" "ssm_access_policy" {
  name        = "lambda_ssm_access"
  description = "Allow Lambda to access SSM parameters"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "ssm:GetParameter",
        "ssm:GetParameters"
      ]
      Resource = "arn:aws:ssm:${var.region}:*:parameter/example_parameter"
    }]
  })
}

# Attach SSM policy to the role
resource "aws_iam_role_policy_attachment" "ssm_policy" {
  role       = aws_iam_role.lambda2_exec_role.name
  policy_arn = aws_iam_policy.ssm_access_policy.arn
}


# Create the Lambda function
resource "aws_lambda_function" "lambda_case_id_validation" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = "lambda_case_id_validation"
  role             = aws_iam_role.lambda2_exec_role.arn
  handler          = "index.handler"
  runtime          = "nodejs18.x"
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  memory_size = 128
  timeout     = 3

  environment {
    variables = {
      PARAM_PATH = "/example/parameter"
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.lambda_policy,
    aws_iam_role_policy_attachment.ssm_policy
  ]
}