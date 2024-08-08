import json
import os
from datetime import datetime
import boto3
from botocore.config import Config

def lambda_handler(event, context):
    #Reading environment variables
    destinationRegion = os.environ['destinationRegion']
    destinationKmsKeyId = os.environ['destinationKmsKeyId']
    destinationOptionGroups = json.loads(os.environ['destinationOptionGroups'])
    snapshotProcessName = os.environ['snapshotProcessName']
    copyRequired = 0 #copy will trigger only if copyRequired is 1
    
    rdsClient = boto3.client('rds')
    
    #Get snapshot tags
    snapshotListRsep = rdsClient.describe_db_snapshots(DBSnapshotIdentifier = event['detail']['SourceIdentifier'], SnapshotType = 'manual')

    #Checking if snapshot was created by the automation process
    if 'DBSnapshots' in snapshotListRsep:
        for DBSnapshot in snapshotListRsep['DBSnapshots']:
            if 'TagList' in DBSnapshot:
                for SnapshotTag in DBSnapshot['TagList']:
                    if SnapshotTag['Key'] == 'SnapshotProcessName' and SnapshotTag['Value'] == snapshotProcessName:
                        copyRequired = 1

    if copyRequired == 1:
        print(f"Copying Snapshot {event['detail']['SourceIdentifier']} from region {os.environ['AWS_REGION']} to {destinationRegion}")

        #Setting up connection for destination region
        rdsDestClientConfig = Config(region_name = destinationRegion)
        rdsDestClient = boto3.client('rds', config = rdsDestClientConfig)

        #Assumption is that snapshot identifier is DBIdentifier + '-' followed by some suffix
        DBIdentifier = '-'.join(event['detail']['SourceIdentifier'].split('-')[:-1])
        
        if DBIdentifier in destinationOptionGroups:
            copySnapshotResp = rdsDestClient.copy_db_snapshot(
                    SourceDBSnapshotIdentifier = event['detail']['SourceArn'],
                    TargetDBSnapshotIdentifier = event['detail']['SourceIdentifier'],
                    KmsKeyId = destinationKmsKeyId,
                    CopyTags = True,
                    OptionGroupName = destinationOptionGroups[DBIdentifier],
                    SourceRegion = os.environ['AWS_REGION']
                )
        else:
            copySnapshotResp = rdsDestClient.copy_db_snapshot(
                    SourceDBSnapshotIdentifier = event['detail']['SourceArn'],
                    TargetDBSnapshotIdentifier = event['detail']['SourceIdentifier'],
                    KmsKeyId = destinationKmsKeyId,
                    CopyTags = True,
                    SourceRegion = os.environ['AWS_REGION']
                )
    else:
        print(f"Skipping cross region snapshot copy for {event['detail']['SourceIdentifier']} as it is not managed with the automation process")

    return {
        'statusCode': 200,
        'body': json.dumps('Execution completed')
    }
