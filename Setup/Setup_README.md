    Fresh AWS setup:
    Create an addmin account in AWS and update the `private_dwh.cfg` with the access Keys for that account
    In us-west-2 give a VPC Public Access
        AWS -> Top left select `us-west-2` -> "VPC dashboard" -> "Security groups" -> select a "Security group ID" -> "Edit inbout rules" -> "Add rule"
            All traffic, All, 0 - 65535, Anywhere-IPv4, 0.0.0.0/0 
            -> "Save rules"
        "Outbound rules" -> "Edit outbound rules"
            All traffic, All, All, Anywhere-IPv4, 0.0.0.0/0 
            -> "Save rules"
        Note the "Security group ID" and update it in `private_dwh.cfg`


    Start Redshfit run:
        python ./Redshift_setup/aws_setup.py

    Connect to the Redshift cluster with Dbeaver: https://youtu.be/s8HckCTC6aA?t=536

    Create The tables Run:
        python .\Redshift_setup\create_tables.py

    Shutdown the Redshift Server when done:
        python.exe .\Redshift_Setup\cleanup_aws.py


    Setup ERM envierment
    install AWS CLI
    Delete ~/.aws/credentials
    run: aws configure
        enter creds from private_dwh.cfg
            Region = us-west-2
    Run `aws sts get-caller-identity` to make sure it is configured correctly

    Create Amazon EC2 key pair run from batch:
        aws ec2 create-key-pair ^
        --key-name spark-cluster ^
        --key-type rsa ^
        --key-format pem ^
        --query "KeyMaterial" ^
        --output text > C:\Users\abigoney\spark-cluster.pem

    Update EMR_Setup\Create_EMR.bat With a subnet in the same VPC as Redshfit

    Create the EMR Run:
        EMR_Setup\Create_EMR.bat

    Enable SSH:
        Once the you created the first cluster go to in AWS
            AWS -> Make sure you are on 'us-west-2' -> `EMR` -> a Cluster ID -> the VPC -> `Security groups` -> Check the group named `ElasticMapReduce-master` -> `Inbound rules` -> `Edit inbound rules` -> `Add rule`
                SSH, Anywhere-IPv4, 0.0.0.0/0  -> `Save rules`

    Lets make a bucket for Logging
        AWS -> S3 -> `Create bucket` -> AWS Region = us-west-2 -> make a bucket name -> `Create bucket`
        Update `Create_EMR.bat` with the new bucket name

    In AWS go to the the cluster and click "Connect to the Primary node using SSH"
    Copy the 'Mac/Linux' comand and past it into powershell
    Change the 'ssh' to 'sftp'
    i.e.: sftp -i ~/spark-cluster.pem hadoop@ec2-35-92-49-166.us-west-2.compute.amazonaws.com
    #run these to test
    SFTP to the master node
    sftp 
        lcd Project
        put dl.cfg
        put Org_etl.py
        exit


    #SSH to the master node
    change the 'sftp' to 'ssh'
    spark-submit --master yarn ./Org_etl.py














Quick refrance:
    cd Setup
    Start Redshfit run:
        python ./Redshift_setup/01_aws_setup.py

    Create The tables Run:
        python .\Redshift_setup\02_create_tables.py

    Shutdown the Redshift Server when done:
        python.exe .\Redshift_Setup\05_cleanup_aws.py


    python.exe .\EMR_Setup\03_Create_EMR.py
    or
    python.exe .\EMR_Setup\04_Rerun.py

    #SSH to the master node
        pip install configparser
        spark-submit --master yarn ./etl.py