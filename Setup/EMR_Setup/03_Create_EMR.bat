@ECHO OFF
aws emr create-cluster ^
    --name udacity_capstone ^
    --use-default-roles ^
    --release-label emr-5.28.0 ^
    --instance-count 1 ^
    --applications Name=Spark Name=Zeppelin ^
    --ec2-attributes KeyName=spark-cluster,SubnetId=subnet-01c397b90507476d9 ^
    --instance-type m5.xlarge ^
    --log-uri s3://andrews-logging/emrlogs/ ^
    --auto-terminate


rem --bootstrap-actions Path="file://C:\Users\abigoney\Documents\School\NanoDegree\Capstone\Setup\EMR_Setup\bootstrap_emr.sh" ^


