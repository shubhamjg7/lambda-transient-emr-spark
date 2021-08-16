import json
import boto3


client = boto3.client('emr')

"""
spark-submit --deploy-mode cluster --master yarn s3://spark-job-cluster-bucket/output/ s3://spark-job-cluster-bucket/input/customers.csv s3://spark-job-cluster-bucket/output/
"""

def lambda_handler(event, context):
    
    response = client.run_job_flow(
        Name= 'spark_job_cluster',
        LogUri= 's3://spark-job-cluster-bucket/logs/',
        ReleaseLabel= 'emr-6.0.0',
        Instances={
            'InstanceGroups': [{
                        'Name': "MasterNode",
                        'Market': 'SPOT',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
            },
            {
                        'Name': "SlaveNode",
                        'Market': 'SPOT',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 2,
            }],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-db57aba6',
            'Ec2KeyName': 'ohio-key'
        },
        Applications = [ {'Name': 'Spark'} ],
        Configurations = [ 
            { 'Classification': 'spark-hive-site',
              'Properties': { 
                  'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'}
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole = 'EMR_EC2_DefaultRole',
        ServiceRole = 'EMR_DefaultRole',
        Steps=[
            {
                'Name': 'flow-log-analysis',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            '/usr/bin/spark-submit',
                            '--deploy-mode', 'cluster',
                            '--master', 'yarn',
                            's3://spark-job-cluster-bucket/scripts/emr_job_script.py',
                            's3://spark-job-cluster-bucket/input/customers.csv',
                            's3://spark-job-cluster-bucket/output/'
                        ]
                }
            }
        ]
    )