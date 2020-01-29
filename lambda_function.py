import logging
import boto3
import json
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)
TIMESTAMP_FORMAT = '%Y-%m-%d-%H-%M'
now = datetime.now()
timestamp_format = now.strftime(TIMESTAMP_FORMAT)

client = boto3.client('rds')
def lambda_handler(event, context):
    try:
        allInstances = client.describe_db_instances()
        shortInfoInstances = {}
        for instances in allInstances['DBInstances']:
            logger.info('Instances info')
            #logger.info(instances)
            shortInfoInstances[instances['DBInstanceIdentifier']]={'DBInstanceIdentifier':instances['DBInstanceIdentifier'],'DBInstanceArn':instances['DBInstanceArn']}
            tag_resource = obtain_log_for_resource(instances['DBInstanceArn'])
            if(search_tag_backup(tag_resource)):
                logger.info('crear snaphot')
                create_db_snapshot(instances)
    except Exception as e:
        raise e
    return { 'statusCode':200,
        'body' : json.dumps(shortInfoInstances) }  
        
        
def create_db_snapshot(instance):
    snapshot_identifier = '%s-%s' % (instance['DBInstanceIdentifier'], timestamp_format)
    try:
        response = client.create_db_snapshot(
                    DBSnapshotIdentifier=snapshot_identifier,
                    DBInstanceIdentifier=instance['DBInstanceIdentifier'],
                    Tags=[{'Key': 'CreatedBy', 'Value': 'lambda-rds-to-take-snapshot'}, {
                        'Key': 'CreatedOn', 'Value': timestamp_format}])
        logger.info('snapshot created')
    except Exception as e:
    
        logger.info('Could not create snapshot %s (%s)' % (snapshot_identifier, e))
def search_tag_backup(tagList):
    try:
        for tag in tagList['TagList']:
            if tag['Key'] == 'take-snapshot' and tag['Value'] == 'true': return True
    except Exception: return False
    else:
        return False

def get_timestamp(snapshot_identifier, snapshot_list):
# Searches for a timestamp on a snapshot name
    pattern = '%s-(.+)' % snapshot_list[snapshot_identifier]['DBInstanceIdentifier']
    date_time = re.search(pattern, snapshot_identifier)

    if date_time is not None:

        try:
            return datetime.strptime(date_time.group(1), _TIMESTAMP_FORMAT)

        except Exception:
            return None

    return None



def obtain_log_for_resource(arn):
    
    response = client.list_tags_for_resource(ResourceName=arn)
    return response
