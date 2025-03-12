

variable "application" {
  type        = string
  description = "name of this application"
  default     = "BCPS-BRIDGE"
}

variable "customer" {
	type        = string
	description = "Customer"
	default     = "PRIME"
}
variable "region" {
  type        = string
  description = "AWS Region, where to deploy cluster."
  default     = "ca-central-1"
}
variable "env" {
  type        = string
  description = "Suffix appended to all managed resource names to indicate their environment"
  default     = "DEV"
}

variable "env_lowercase" {
  type        = string
  description = "env in lowercase"
  default     = "dev"
}

variable "env_full" {
  type        = string
  description = "full name of environment (i.e. INTEGRATION)"
  default     = "development"
}

#SQS-specific variables

variable "queueName" {
	type = string
	description="Name of this SQS queue"
	default="q-prime-rms-file-transfer.fifo"
}
variable "clamQueue" {
  type    = string
  default = null
}

variable "maxReceivedCount" {
  type        = number
  description = "How many messages can be placed into the deadletter queue"
  default     = 4
}

variable "queue_name" {
  type    = string
  default = "q-prime-rms-file-transfer.fifo" # Must end in .fifo for FIFO queues
}

variable "message_retention_seconds" {
  type    = number
  default = 345600 # 4 days
}

variable "visibility_timeout_seconds" {
  type    = number
  default = 30
}

variable "receive_wait_time_seconds" {
  type    = number
  default = 0
}