import json
import boto3
from botocore.config import Config
from datetime import datetime, timedelta
import time
import os

def lambda_handler(event, context):
    rdsClient = boto3.client('rds')
    
    destinationRegion = os.environ['destinationRegion']
    includeDatabases = json.loads(os.environ['includeDatabases'])
    excludeDatabases = json.loads(os.environ['excludeDatabases']) #exclude is ignored if include list is provided
    snapshotProcessName = os.environ['snapshotProcessName']
    tagAdditionRequired = 0 #Tags will be added only if tagAdditionRequired is 1
    pageToken = "start"

    if len(includeDatabases) == 1:
        if len(includeDatabases[0]) == 0:
            includeDatabases = []
            
    if len(excludeDatabases) == 1:
        if len(excludeDatabases[0]) == 0:
            excludeDatabases = []
    
    print(f"Include list of databases: {includeDatabases}")
    print(f"Exclude list of databases: {excludeDatabases}")
    print(f"Starting tagging in primary region: {os.environ['AWS_REGION']}")
    
    while pageToken != "":  
        if len(includeDatabases) == 0:
            if pageToken == 'start':
                snapshotListRsep = rdsClient.describe_db_snapshots(SnapshotType = 'manual', MaxRecords = 100)
            else:
                snapshotListRsep = rdsClient.describe_db_snapshots(SnapshotType = 'manual', MaxRecords = 100, Marker = pageToken)
        else:
            if pageToken == 'start':
                snapshotListRsep = rdsClient.describe_db_snapshots(SnapshotType = 'manual', MaxRecords = 100, Filters = [{'Name': 'db-instance-id', 'Values': includeDatabases}])
            else:
                snapshotListRsep = rdsClient.describe_db_snapshots(SnapshotType = 'manual', MaxRecords = 100, Marker = pageToken, Filters = [{'Name': 'db-instance-id', 'Values': includeDatabases}])                

        if 'Marker' in snapshotListRsep:
            pageToken = snapshotListRsep['Marker']
        else:
            pageToken = ""
    
        #Getting list of DB Snapshots to tag
        if 'DBSnapshots' in snapshotListRsep:
            for DBSnapshot in snapshotListRsep['DBSnapshots']:
                if (DBSnapshot['DBInstanceIdentifier'] not in excludeDatabases or len(includeDatabases) > 0):
                    tagAdditionRequired = 1
                    if 'TagList' in DBSnapshot:
                        for SnapshotTag in DBSnapshot['TagList']:
                            if SnapshotTag['Key'] == 'SnapshotProcessName':
                                tagAdditionRequired = 0

                    if tagAdditionRequired == 1:
                        print(f"Adding tags to snapshot {DBSnapshot['DBSnapshotArn']} (databse: {DBSnapshot['DBInstanceIdentifier']}, snapshot creation date: {DBSnapshot['OriginalSnapshotCreateTime'].strftime('%Y-%m-%d %H:%M:%S')})")
                        response = rdsClient.add_tags_to_resource(ResourceName = DBSnapshot['DBSnapshotArn'], Tags = [{"Key": "SnapshotProcessName", "Value": snapshotProcessName}])
                    else:
                        print(f"Skipping to add tags for snapshot {DBSnapshot['DBSnapshotArn']} (databse: {DBSnapshot['DBInstanceIdentifier']}) as tag named 'SnapshotProcessName' already exists")
    
    print(f"Starting clean-up in secondary region: {destinationRegion}")
    
    #Setting up connection for destination region
    rdsDestClientConfig = Config(region_name = destinationRegion)
    rdsDestClient = boto3.client('rds', config = rdsDestClientConfig)
    pageToken = "start"

    while pageToken != "":  
        if len(includeDatabases) == 0:
            if pageToken == 'start':
                snapshotListRsep = rdsDestClient.describe_db_snapshots(SnapshotType = 'manual', MaxRecords = 100)
            else:
                snapshotListRsep = rdsDestClient.describe_db_snapshots(SnapshotType = 'manual', MaxRecords = 100, Marker = pageToken)
        else:
            if pageToken == 'start':
                snapshotListRsep = rdsDestClient.describe_db_snapshots(SnapshotType = 'manual', MaxRecords = 100, Filters = [{'Name': 'db-instance-id', 'Values': includeDatabases}])
            else:
                snapshotListRsep = rdsDestClient.describe_db_snapshots(SnapshotType = 'manual', MaxRecords = 100, Marker = pageToken, Filters = [{'Name': 'db-instance-id', 'Values': includeDatabases}])                

        if 'Marker' in snapshotListRsep:
            pageToken = snapshotListRsep['Marker']
        else:
            pageToken = ""
    
        #Getting list of DB Snapshots to tag
        if 'DBSnapshots' in snapshotListRsep:
            for DBSnapshot in snapshotListRsep['DBSnapshots']:
                if (DBSnapshot['DBInstanceIdentifier'] not in excludeDatabases or len(includeDatabases) > 0):
                    tagAdditionRequired = 1
                    if 'TagList' in DBSnapshot:
                        for SnapshotTag in DBSnapshot['TagList']:
                            if SnapshotTag['Key'] == 'SnapshotProcessName':
                                tagAdditionRequired = 0

                    if tagAdditionRequired == 1:
                        print(f"Adding tags to snapshot {DBSnapshot['DBSnapshotArn']} (databse: {DBSnapshot['DBInstanceIdentifier']}, snapshot creation date: {DBSnapshot['OriginalSnapshotCreateTime'].strftime('%Y-%m-%d %H:%M:%S')})")
                        response = rdsDestClient.add_tags_to_resource(ResourceName = DBSnapshot['DBSnapshotArn'], Tags = [{"Key": "SnapshotProcessName", "Value": snapshotProcessName}])
                    else:
                        print(f"Skipping to add tags for snapshot {DBSnapshot['DBSnapshotArn']} (databse: {DBSnapshot['DBInstanceIdentifier']}) as tag named 'SnapshotProcessName' already exists")


    return {
        'statusCode': 200,
        'body': "Completed"
    }
