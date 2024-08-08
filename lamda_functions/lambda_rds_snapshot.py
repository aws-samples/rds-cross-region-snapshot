import json
import boto3
import botocore
from datetime import datetime
import time
import os

def lambda_handler(event, context):
    rdsClient = boto3.client('rds')
    currentDateTime = datetime.today().strftime('%Y%m%d%H%M%S')
    returnMessage = {}
    returnMessage['failureCount'] = 0
    returnMessage['successCount'] = 0
    
    DBList = []
    retryLimit = int(os.environ['retryCount'])
    secondsBetweenRetries = int(os.environ['secondsBetweenRetries'])
    snapshotProcessName = os.environ['snapshotProcessName']
    includeDatabases = json.loads(os.environ['includeDatabases'])
    excludeDatabases = json.loads(os.environ['excludeDatabases']) #exclude is ignored if include list is provided

    if len(includeDatabases) == 1:
        if len(includeDatabases[0]) == 0:
            includeDatabases = []

    if len(excludeDatabases) == 1:
        if len(excludeDatabases[0]) == 0:
            excludeDatabases = []
    
    print(f"Include list of databases: {includeDatabases}")
    print(f"Exclude list of databases: {excludeDatabases}")
    
    dbListRep = rdsClient.describe_db_instances()
    
    #Getting list of DB instances to backup
    if 'DBInstances' in dbListRep:
        for DB in dbListRep['DBInstances']:
            if (DB['DBInstanceIdentifier'] in includeDatabases or len(includeDatabases) == 0) and (DB['DBInstanceIdentifier'] not in excludeDatabases or len(includeDatabases) > 0):
                DBList.append(DB['DBInstanceIdentifier'])
            elif DB['DBInstanceIdentifier'] in excludeDatabases and len(includeDatabases) == 0:
                print(f"Ignoring database {DB['DBInstanceIdentifier']} as it is in the exclude list")
            elif DB['DBInstanceIdentifier'] not in includeDatabases:
                print(f"Ignoring database {DB['DBInstanceIdentifier']} as it is not in the include list")
                
    for DBIdentifier in DBList:
        sapshotIdentifier = DBIdentifier + '-' + currentDateTime
        retryCounter = 0
        failureCode = 1
        
        returnMessage[sapshotIdentifier] = "In Progress"
        
        while failureCode != 0 and retryCounter < retryLimit:
            try:
                retryCounter = retryCounter + 1
                
                #add time delay if it is not the first trype
                if retryCounter > 1:
                    print(f'Retrying after {secondsBetweenRetries} seconds')
                    time.sleep(secondsBetweenRetries)
                
                print(f"Creating manual snapshot {sapshotIdentifier} of database: {DBIdentifier}")
    
                createSnapshotResp = rdsClient.create_db_snapshot(
                    DBSnapshotIdentifier = sapshotIdentifier,
                    DBInstanceIdentifier = DBIdentifier,
                    Tags=[
                        {
                            'Key': 'SnapshotStartTime',
                            'Value': currentDateTime
                        },
                        {
                            'Key': 'SnapshotProcessName',
                            'Value': snapshotProcessName
                        }
                    ]
                )
                
                failureCode = 0
            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == 'InvalidDBInstanceStateFault':
                    print(f'DB instance: {DBIdentifier} is not in Available state.')
                elif error.response['Error']['Code'] == 'InvalidDBInstanceState':
                    print(f'Earlier Snpashot for the DB instance: {DBIdentifier} is in progress.')
                else:
                    raise error
                
        if failureCode != 0:
            returnMessage[sapshotIdentifier] = "Failed"
            returnMessage['failureCount'] = returnMessage['failureCount'] + 1
            
            print(f"Snapshot for database {DBIdentifier} failed after retrying {retryLimit} times. Skipping....")
        else:
            returnMessage[sapshotIdentifier] = "Succeeded"
            returnMessage['successCount'] = returnMessage['successCount'] + 1

    if returnMessage['successCount'] == 0 and len(DBList) > 0:
        raise Exception(f"All the snapshots failed after retrying {retryLimit} times. Exitting...")
    
    return {
        'statusCode': 200,
        'body': returnMessage
    }
