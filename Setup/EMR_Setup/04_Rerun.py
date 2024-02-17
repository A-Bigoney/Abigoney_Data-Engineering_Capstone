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

sftp.put('C:\\Users\\abigoney\\Documents\\School\\NanoDegree\\Capstone\\Project\\sql_dim_tables.py', 'sql_dim_tables.py')

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


