Fresh AWS Setup
Step 1: Create an admin account in AWS IAM and update the private_dwh.cfg file with the access keys for that account.

Step 2: Setup AWS CLI
    Delete the ~/.aws/credentials file.
    Run the following command:
    aws configure
    Enter the credentials from private_dwh.cfg and set the region to us-west-2.
    Run the command aws sts get-caller-identity to verify that the configuration is correct.

Step 3: Give VPC Public Access in us-west-2
    Go to AWS and select us-west-2.
    Navigate to "VPC dashboard" -> "Security groups".
    Select a "Security group ID" and click "Edit inbound rules".
    Add the following inbound rule:
    All traffic, All, 0 - 65535, Anywhere-IPv4, 0.0.0.0/0
    Click "Save rules".
    Click "Edit outbound rules" and add the following outbound rule:
    All traffic, All, All, Anywhere-IPv4, 0.0.0.0/0
    Click "Save rules".
    Note the "Security group ID" and update it in private_dwh.cfg.

Step 4: Start Redshift
    Navigate to the Setup folder.
    Run the following command:
    `python ./Redshift_setup/01_aws_setup.py`

Step 5: Create the tables
    Run the following command:
        `python ./Redshift_setup/02_create_tables.py`

Step 6: Shutdown the Redshift server when done
    Run the following command:
    `python ./Redshift_Setup/05_cleanup_aws.py`

Step 7: Setup EMR environment
    Create an Amazon EC2 key pair by running the following command:
    `aws ec2 create-key-pair --key-name spark-cluster --key-type rsa --key-format pem --query "KeyMaterial" --output text > C:\Users\abigoney\spark-cluster.pem`
    Create EMR Default Roles by running the following command:
    `aws emr create-default-roles`
    Update the --ec2-attributes line in EMR_Setup/03_Create_EMR.py with a subnet in the same VPC as Redshift.

Step 8: Enable SSH
    Manually create a cluster in AWS.
    Go to AWS -> EMR -> Cluster ID -> VPC -> Security groups.
    Check the group named ElasticMapReduce-master and click "Edit inbound rules".
    Add the following inbound rule:
    SSH, Anywhere-IPv4, 0.0.0.0/0
    Click "Save rules".
    Terminate the manually created cluster.

Step 9: Create a bucket for EMR Logging
    Go to AWS -> S3 -> "Create bucket".
    Set the AWS Region to us-west-2 and choose a bucket name.
    Click "Create bucket".
    Update the --log-uri line in EMR_Setup/03_Create_EMR.py with the new bucket name.

Step 10: Create the EMR cluster
    Run the following command:
    `python ./EMR_Setup/03_Create_EMR.py`
    Running Manually (Not needed if running 03_Create_EMR.py)
    Update Setup/dwh.cfg with the Redshift cluster information.
    Go to AWS EMR, select the cluster, and click "Connect to the Primary node using SSH".
    Copy the 'Mac/Linux' command and paste it into PowerShell.
    Change 'ssh' to 'sftp'.
    Example: sftp -i ~/spark-cluster.pem hadoop@ec2-35-92-49-166.us-west-2.compute.amazonaws.com
    Run the following commands to test:
    `sftp`
    `lcd Project`
    `put etl.py`
    `cd ..`
    `cd Setup`
    `put dwh.cfg`
    `exit`
    Change 'sftp' to 'ssh' and run the following command:
    `spark-submit --master yarn ./etl.py`


Quick Reference
To start Redshift: 
    `python ./Redshift_setup/01_aws_setup.py`
To create the tables: 
    `python ./Redshift_setup/02_create_tables.py`
To shutdown the Redshift server: 
    `python ./Redshift_Setup/05_cleanup_aws.py`
To create the EMR cluster: 
    `python ./EMR_Setup/03_Create_EMR.py'
        or
    'python ./EMR_Setup/04_Rerun.py`
To SSH to the master node: 
    Run the SSH command provided in the AWS EMR console or by the 03_Create_ERM.py or 04_Rerun.py scripts.
To run the ETL process manually: 
    Update `Setup/dwh.cfg` and run `spark-submit --master yarn ./etl.py`
