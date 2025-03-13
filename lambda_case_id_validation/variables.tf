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

variable "dems_isl_api_key" {
  type        = string
  description = "DEMS ISL API access key"
  default     = "7N6EYCR6TD88O7MV1VXQNBP0K"
}

variable "dems_isl_api_url" {
  type        = string
  description = "DEMS ISL API URL"
  default     = "http://zb-rp.dev.ag.gov.bc.ca/jade-ccm-justin/api/v1"
}

variable "case_id_dynamo_table_name" {
  type        = string
  description = "dynamo table name"
  default     = ""
}

variable "sqs_file_transfer_name" {
  type        = string
  description = "sqs file transfer name"
  default     = ""
}



