resource "aws_db_instance" "rds_postgresql" {
  allocated_storage    = 20
  engine              = "postgres"
  engine_version      = "14"
  instance_class      = "db.t3.medium"
  username           = var.db_username
  password           = var.db_password
  publicly_accessible = false
  skip_final_snapshot = true
}
