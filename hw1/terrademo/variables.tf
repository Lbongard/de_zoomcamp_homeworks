variable "credentials" {
  description = "Credentials to access GCP project"
  default = "./keys/my-creds.json"
}

variable "project" {
  description = "Project"
  default     = "inbound-ship-412204"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}


variable "bq_dataset_name" {
  description = "My BigQuery Dataet Name"
  default     = "hw1_dataset"
}

variable "gcs_buckt_name" {
  description = "My Storage Bucket Name"
  default     = "inbound-ship-412204-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}