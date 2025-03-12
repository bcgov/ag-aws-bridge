# Configure the AWS provider
provider "aws" {
  region = var.region
}

# Get the current AWS account information
data "aws_caller_identity" "current" {}

# IAM Role for Lambda and API Gateway to assume
resource "aws_iam_role" "bridge_sqs_role" {
  name = "${var.application}-iam-role-prime-rms-sqs-${var.env}"
  tags = {
    Application = var.application
    Customer    = var.customer
    Environment = var.env
  }
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action    = "sts:AssumeRole"
        Effect    = "Allow"
        Principal = {
          Service = [
            "lambda.amazonaws.com",
            "apigateway.amazonaws.com"
          ]
        }
      }
    ]
  })
}

# IAM Policy granting SQS permissions to the role
resource "aws_iam_role_policy" "sqs_access_policy" {
  name   = "${var.application}-sqs-access-policy-${var.env}"
  role   = aws_iam_role.bridge_sqs_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sqs:SendMessage",
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes"
        ]
        Resource = aws_sqs_queue.q-prime-rms-file-transfer-fifo.arn
      }
    ]
  })
}

# Create the SQS FIFO queue
resource "aws_sqs_queue" "q-prime-rms-file-transfer-fifo" {
  depends_on                  = [aws_iam_role.bridge_sqs_role]
  name                        = var.queue_name # Renamed variable for clarity
  fifo_queue                  = true
  content_based_deduplication = false
  delay_seconds               = 0
  message_retention_seconds   = var.message_retention_seconds
  visibility_timeout_seconds  = var.visibility_timeout_seconds
  receive_wait_time_seconds   = var.receive_wait_time_seconds

  tags = {
    Environment = var.env
    Purpose     = "File Transfer Queue" # Updated empty string
    AccountID   = data.aws_caller_identity.current.account_id
  }
}

# SQS Queue Policy to allow the IAM role to access the queue
resource "aws_sqs_queue_policy" "q-prime-rms-file-transfer-policy" {
  queue_url = aws_sqs_queue.q-prime-rms-file-transfer-fifo.id
  policy    = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = {
          AWS = aws_iam_role.bridge_sqs_role.arn
        }
        Action    = [
          "sqs:SendMessage",
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage"
        ]
        Resource  = aws_sqs_queue.q-prime-rms-file-transfer-fifo.arn
      }
    ]
  })
}

# Output the queue URL
output "queue_url" {
  value       = aws_sqs_queue.q-prime-rms-file-transfer-fifo.id
  description = "The URL of the created SQS FIFO queue"
}

# Output the queue ARN
output "queue_arn" {
  value       = aws_sqs_queue.q-prime-rms-file-transfer-fifo.arn
  description = "The ARN of the created SQS FIFO queue"
}