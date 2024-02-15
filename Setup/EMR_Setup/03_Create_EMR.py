import os
import json
import pandas as pd
import time

#df = pd.read_json(os.system(".\EMR_Setup\03_Create_EMR.bat"))
#df = 
#print(os.popen("C:\\Users\\abigoney\\Documents\\School\\NanoDegree\\Capstone\\Setup\\EMR_Setup\\03_Create_EMR.bat").read())

#print(os.popen("aws emr create-cluster --name udacity_capstone --use-default-roles --release-label emr-5.28.0 --instance-count 1 --applications Name=Spark Name=Zeppelin --ec2-attributes KeyName=spark-cluster,SubnetId=subnet-01c397b90507476d9 --instance-type m5.xlarge --log-uri s3://andrews-logging/emrlogs/ --auto-terminate > ClusterID.json").read())

#Run without Auto-Termaininatte !!!!!!!!!!!!!!!!!!!!! BE SURE TO SHUT IT DOWN WHEN DONE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#print(os.popen("""aws emr create-cluster \
#                --name udacity_capstone \
#                --use-default-roles \
#                --release-label "emr-5.36.1" \
#                --instance-count 1 \
#                --applications Name=Spark Name=Zeppelin \
#                --ec2-attributes KeyName=spark-cluster,SubnetId=subnet-01c397b90507476d9 \
#                --instance-type m5.xlarge \
#                --log-uri s3://andrews-logging/emrlogs/ > ClusterID.json""").read())

print(os.popen("""aws emr create-cluster \
                --name udacity_capstone \
                --use-default-roles \
                --release-label "emr-6.15.0" \
                --instance-count 1 \
                --applications Name=Spark Name=Zeppelin \
                --ec2-attributes KeyName=spark-cluster,SubnetId=subnet-01c397b90507476d9 \
                --instance-type m5.xlarge \
                --log-uri s3://andrews-logging/emrlogs/ > ClusterID.json""").read())


with open('ClusterID.json') as file:
    data = json.load(file)

df = pd.DataFrame(data, index=[0])
cluster_id = df.loc[0, 'ClusterId']
print("ClusterID: ", cluster_id)
print("Sleeping for 2.5 minutes while the cluster boots")
time.sleep(30)
print("120 Seconds remaining")
time.sleep(30)
print("90 Seconds remaining")
time.sleep(30)
print("60 Seconds remaining")
time.sleep(30)
print("30 Seconds remaining")
time.sleep(20)
print("10 Seconds remaining")
time.sleep(10)

import os
import json
import pandas as pd
import time


with open('ClusterID.json') as file:
    data = json.load(file)

df = pd.DataFrame(data, index=[0])
cluster_id = df.loc[0, 'ClusterId']

command = f"aws emr describe-cluster --output text --cluster-id {cluster_id} --query Cluster.MasterPublicDnsName"

hostname=os.popen(command).read()
print(f"sftp -i ~/spark-cluster.pem hadoop@{hostname}")

print(f"Hostname={hostname}")
import paramiko
ssh_client = paramiko.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
private_key_path = "C:\\Users\\abigoney\\spark-cluster.pem"
private_key = paramiko.RSAKey.from_private_key_file(private_key_path)
myhostname = hostname.rstrip('\n')
ssh_client.connect(hostname=myhostname,
                   username='hadoop',
                   pkey=private_key)


sftp = ssh_client.open_sftp()

sftp.put('C:\\Users\\abigoney\\Documents\\School\\NanoDegree\\Capstone\\Setup\\dwh.cfg', 'dwh.cfg')

sftp.put('C:\\Users\\abigoney\\Documents\\School\\NanoDegree\\Capstone\\Project\\etl.py', 'etl.py')
sftp.put('C:\\Users\\abigoney\\Documents\\School\\NanoDegree\\Capstone\\Project\\run.sh', 'run.sh')

sftp.close()
ssh_client.close()


print(f"sftp -i ~/spark-cluster.pem hadoop@{hostname}")
#print("put dwh.cfg")
#print("lcd Project")
#print("put etl.py")
#print("exit")
print(f"ssh -i ~/spark-cluster.pem hadoop@{hostname}")
print("yes")
print("spark-submit --master yarn ./etl.py")