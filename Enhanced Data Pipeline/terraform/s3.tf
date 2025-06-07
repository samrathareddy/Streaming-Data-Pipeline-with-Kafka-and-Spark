resource "aws_s3_bucket" "data_lake" {
  bucket = "end-to-end-pipeline-data-lake"
  acl    = "private"
}
