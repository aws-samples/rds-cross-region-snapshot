import json
import boto3
from botocore.config import Config
from datetime import datetime, timedelta
import time
import os

def lambda_handler(event, context):
    rdsClient = boto3.client('rds')
    
    retentionInDays = int(os.environ['retentionInDays'])
    destinationRegion = os.environ['destinationRegion']
    includeDatabases = json.loads(os.environ['includeDatabases'])
    excludeDatabases = json.loads(os.environ['excludeDatabases']) #exclude is ignored if include list is provided
    snapshotProcessName = os.environ['snapshotProcessName']
    deleteRequired = 0 #delete will trigger only if deleteRequired is 1
    pageToken = "start"
    
    deleteSnapshotsDate = datetime.utcnow().astimezone() - timedelta(days = retentionInDays)

    if len(includeDatabases) == 1:
        if len(includeDatabases[0]) == 0:
            includeDatabases = []
            
    if len(excludeDatabases) == 1:
        if len(excludeDatabases[0]) == 0:
            excludeDatabases = []
    
    print(f"The snapshots with creation date before timestamp: {deleteSnapshotsDate.strftime('%Y-%m-%d %H:%M:%S')} would be deleted (retention period: {retentionInDays} days)")
    print(f"Include list of databases: {includeDatabases}")
    print(f"Exclude list of databases: {excludeDatabases}")
    print(f"Starting clean-up in primary region: {os.environ['AWS_REGION']}")
    
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
    
        #Getting list of DB Snapshots to delete
        if 'DBSnapshots' in snapshotListRsep:
            for DBSnapshot in snapshotListRsep['DBSnapshots']:
                if (DBSnapshot['DBInstanceIdentifier'] not in excludeDatabases or len(includeDatabases) > 0) and DBSnapshot['OriginalSnapshotCreateTime'] < deleteSnapshotsDate and DBSnapshot['Status'] == 'available':
                    deleteRequired = 0
                    if 'TagList' in DBSnapshot:
                        for SnapshotTag in DBSnapshot['TagList']:
                            if SnapshotTag['Key'] == 'SnapshotProcessName' and SnapshotTag['Value'] == snapshotProcessName:
                                deleteRequired = 1

                    if deleteRequired == 1:
                        print(f"Deleting snapshot {DBSnapshot['DBSnapshotIdentifier']} (databse: {DBSnapshot['DBInstanceIdentifier']}, snapshot creation date: {DBSnapshot['OriginalSnapshotCreateTime'].strftime('%Y-%m-%d %H:%M:%S')})")
                        response = rdsClient.delete_db_snapshot(DBSnapshotIdentifier=DBSnapshot['DBSnapshotIdentifier'])
                    else:
                        print(f"Skipping to delete snapshot {DBSnapshot['DBSnapshotIdentifier']} (databse: {DBSnapshot['DBInstanceIdentifier']}) as it is not managed by the automation process")
    
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
    
        #Getting list of DB Snapshots to delete
        if 'DBSnapshots' in snapshotListRsep:
            for DBSnapshot in snapshotListRsep['DBSnapshots']:
                if (DBSnapshot['DBInstanceIdentifier'] not in excludeDatabases or len(includeDatabases) > 0) and DBSnapshot['OriginalSnapshotCreateTime'] < deleteSnapshotsDate and DBSnapshot['Status'] == 'available':
                    deleteRequired = 0
                    if 'TagList' in DBSnapshot:
                        for SnapshotTag in DBSnapshot['TagList']:
                            if SnapshotTag['Key'] == 'SnapshotProcessName' and SnapshotTag['Value'] == snapshotProcessName:
                                deleteRequired = 1

                    if deleteRequired == 1:        
                        print(f"Deleting snapshot {DBSnapshot['DBSnapshotIdentifier']} (databse: {DBSnapshot['DBInstanceIdentifier']}, snapshot creation date: {DBSnapshot['OriginalSnapshotCreateTime'].strftime('%Y-%m-%d %H:%M:%S')})")
                        response = rdsDestClient.delete_db_snapshot(DBSnapshotIdentifier=DBSnapshot['DBSnapshotIdentifier'])
                    else:
                        print(f"Skipping to delete snapshot {DBSnapshot['DBSnapshotIdentifier']} (databse: {DBSnapshot['DBInstanceIdentifier']}) as it is not managed by the automation process")

    return {
        'statusCode': 200,
        'body': "Completed"
    }
