
provider "aws" {
  region = "ap-south-1"
}

resource "aws_s3_bucket" "data_bucket" {
  bucket = "test-data-bucket"
}

resource "aws_glue_catalog_database" "glue_db" {
  name = "data_engineering_db"
}
