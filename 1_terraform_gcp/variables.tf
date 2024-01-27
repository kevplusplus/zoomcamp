variable "credentials" {
  description = "Credentials"
  default = "./keys/creds.json"
}

variable "project" {
  description = "Project"
  default     = "fast-banner-412410"
}

variable "region" {
  description = "Region"
  default     = "us-west1"
}

variable "location" {
  description = "Project location"
  default     = "US"

}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "fast-banner-412410-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket storage Class"
  default     = "STANDARD"
}