output "eks_cluster_name" {
  value = aws_eks_cluster.eks.name
}

output "rds_endpoint" {
  value = aws_db_instance.rds_postgresql.endpoint
}

output "s3_bucket_name" {
  value = aws_s3_bucket.data_lake.bucket
}
