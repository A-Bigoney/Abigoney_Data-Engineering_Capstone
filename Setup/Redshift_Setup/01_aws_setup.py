import pandas as pd
import boto3
from botocore.exceptions import ClientError
import json
import configparser
import os
import time

config = configparser.ConfigParser()
config.read('private_dwh.cfg')

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
VPC_SECURITY_GROUP_IDS = config.get('AWS', 'VPC_SECURITY_GROUP_IDS')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

print(pd.DataFrame({"Param":
                ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
            "Value":
                [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
            }))

ec2 = boto3.resource('ec2',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                )

s3 = boto3.resource('s3',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                )

iam = boto3.client('iam',aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET,
                    region_name='us-west-2'
                )

redshift = boto3.client('redshift',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )




#1.1 Create the role, 
try:
    print("1.1 Creating a new IAM Role") 
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
            'Effect': 'Allow',
            'Principal': {'Service': 'redshift.amazonaws.com'}}],
            'Version': '2012-10-17'})
    )    
except Exception as e:
    print(e)
    
    
print("1.2 Attaching Policy")

iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                    PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                    )['ResponseMetadata']['HTTPStatusCode']

print("1.3 Get the IAM role ARN")
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']


print(roleArn)

#Create the cluster
print("1.4 Creat Cluster")
try:
    response = redshift.create_cluster(        
        #HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[roleArn],
        # Publicly accessible
        PubliclyAccessible=True,
        # VPC Security Group
        #Udacit's VpcSecurityGroupIds=['sg-09a288d5f545e1f48']  
        #VpcSecurityGroupIds=['sg-027a6434318b9f0e5']
        VpcSecurityGroupIds=[VPC_SECURITY_GROUP_IDS]
    )
except Exception as e:
    print(e)

#check status of cluster
def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
print(prettyRedshiftProps(myClusterProps))


#Colect ARN
print("I'm going to keep trying ever 30 Seconds till it cluster comes online")
print("This could take 3 or 4 minutes")
while True:
    try:
        DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
        print("Cluster is up!")
        break
    except KeyError:
        print("Cluster is not up Wait 2 Minutes and Press CTRL+C and rerun the script")
        DWH_ENDPOINT = 'null'
        time.sleep(30)


DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

#Delete Old dwh.cfg
if os.path.exists('dwh.cfg'):
    os.remove('dwh.cfg')
else:
    print("No old 'dwh.cfg' to delete.")



#asemble what will be writen out to dwh.cfg
print('Will Write:')
output=['[CLUSTER]','\n',
        'HOST=',DWH_ENDPOINT,'\n',
        'DB_NAME=', DWH_DB,'\n',
        'DB_USER=', DWH_DB_USER,'\n',
        'DB_PASSWORD=', DWH_DB_PASSWORD,'\n',
        'DB_PORT=5439','\n\n',

        '[IAM_ROLE]','\n',
        'ARN=\'',DWH_ROLE_ARN,'\'\n\n',

        '[S3]','\n',
        'INPUT_BUCKET=s3a://andrew-capstone-data/Data_Files\n',
        'TEMP_BUCKET=s3a://andrews-emr-temp/temp\n\n',

        '[AWS]','\n',
        'KEY=',KEY,'\n',
        'SECRET=',SECRET,'\n',
        'VPC_SECURITY_GROUP_IDS=',VPC_SECURITY_GROUP_IDS,'\n'
        ]
        
print(output)

#Create dwh.cfg
dwh_file = open('dwh.cfg', 'w')
for item in output:
    dwh_file.write(item)
dwh_file.close()

