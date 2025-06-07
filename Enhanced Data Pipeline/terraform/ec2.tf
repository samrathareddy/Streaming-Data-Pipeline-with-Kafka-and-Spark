resource "aws_instance" "airflow" {
  ami           = "ami-0abcdef1234567890" # Replace with latest AMI
  instance_type = var.instance_type
  key_name      = "my-key-pair"
  security_groups = ["pipeline-security"]

  tags = {
    Name = "Airflow-Instance"
  }
}

resource "aws_instance" "kafka" {
  ami           = "ami-0abcdef1234567890"
  instance_type = var.instance_type
  key_name      = "my-key-pair"
  security_groups = ["pipeline-security"]

  tags = {
    Name = "Kafka-Instance"
  }
}

resource "aws_instance" "spark" {
  ami           = "ami-0abcdef1234567890"
  instance_type = var.instance_type
  key_name      = "my-key-pair"
  security_groups = ["pipeline-security"]

  tags = {
    Name = "Spark-Instance"
  }
}
